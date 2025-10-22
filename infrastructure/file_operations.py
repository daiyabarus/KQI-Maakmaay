"""
Concrete implementations of repository interfaces
"""
import gzip
import os
import shutil
from pathlib import Path
from typing import List
import pandas as pd
import numpy as np

from core.repositories import IFileRepository, IDataRepository, IKQIProcessor
from core.entities import NetworkIdentifier


class FileRepository(IFileRepository):
    """Concrete implementation of file operations"""
    
    def list_gz_files(self, folder_path: str) -> List[str]:
        """List semua file .csv.gz dalam folder"""
        path = Path(folder_path)
        return sorted([str(f) for f in path.glob("*.csv.gz")])

    def extract_gz_files(self, gz_files: List[str], output_folder: str) -> List[str]:
        """Extract file .csv.gz ke folder output"""
        extracted_files = []

        for gz_file in gz_files:
            output_file = os.path.join(output_folder, Path(gz_file).stem)

            with gzip.open(gz_file, "rb") as f_in:
                with open(output_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            extracted_files.append(output_file)

        return extracted_files

    def create_directory(self, directory_path: str) -> None:
        """Create directory if not exists"""
        Path(directory_path).mkdir(parents=True, exist_ok=True)

    def remove_directory(self, directory_path: str) -> None:
        """Remove directory and all its contents"""
        try:
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
        except Exception as e:
            raise Exception(f"Failed to remove directory {directory_path}: {str(e)}")


class DataRepository(IDataRepository):
    """Concrete implementation of data operations"""
    
    # Column mapping untuk KQI raw (NO HEADER)
    COLUMN_MAPPING = {
        0: 'timecolumn',
        1: 'batchno',
        2: 'cgisai',
        3: 'e2e_delay_per_record',
        4: 'tcp_rtt',
        5: 'tcp_rtt_good_count',
        6: 'streaming_download_throughput',
        7: 'streaming_datatrans_duration',
        8: 'streaming_dw_packets_size',
        9: 'server_probe_dl_lost_rate_per',
        10: 'server_probe_ul_lost_rate_per',
        11: 'user_probe_dl_lost_rate_per',
        12: 'user_probe_ul_lost_rate_per',
        13: 'server_probe_ul_lost_pkt',
        14: 'server_probe_dw_lost_pkt',
        15: 'user_probe_dw_lost_pkt',
        16: 'user_probe_ul_lost_pkt',
        17: 'tcp_ul_packages_withpl',
        18: 'tcp_dl_packages_withpl',
        19: 'user_num',
        20: 'syn_synack_delay_per_record',
        21: 'tcp_rtt_step1',
        22: 'tcp_rtt_step1_good_count'
    }
    
    def load_csv_files(self, csv_files: List[str], delimiter: str) -> pd.DataFrame:
        """Load dan gabungkan multiple CSV files - NO HEADER"""
        dfs = []
        for csv_file in csv_files:
            # Load WITHOUT header (header=None)
            df = pd.read_csv(csv_file, delimiter=delimiter, header=None)
            df['source_file'] = Path(csv_file).name
            dfs.append(df)
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Rename kolom berdasarkan posisi
        col_map = {i: name for i, name in self.COLUMN_MAPPING.items() if i < len(combined_df.columns)}
        combined_df = combined_df.rename(columns=col_map)
        
        return combined_df

    def load_mapping_file(self, mapping_file: str) -> pd.DataFrame:
        """Load mapping file - WITH HEADER, COMMA-separated"""
        # Load WITH header (header=0), explicitly use COMMA separator
        df = pd.read_csv(
            mapping_file, 
            sep=',', 
            header=0,
            engine='python',
            encoding='utf-8',
            skipinitialspace=True
        )
        
        # Clean column names (remove any whitespace)
        df.columns = df.columns.str.strip()
        
        # Required columns only
        required_cols = ['TOWER ID', 'SWE_L5', 'eNodeBId']
        
        # Check if all required columns exist
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            available_cols = list(df.columns)
            raise ValueError(
                f"Mapping file missing required columns: {missing_cols}\n"
                f"Available columns: {available_cols}\n"
                f"Please ensure file is COMMA-separated with header: TOWER ID, SWE_L5, eNodeBId"
            )
        
        # Select and rename only needed columns
        df = df[required_cols].copy()
        df.columns = ['tower_id', 'swe_l5', 'enodeb_id']
        
        # Remove whitespace from ALL string columns
        df['tower_id'] = df['tower_id'].astype(str).str.strip()
        df['swe_l5'] = df['swe_l5'].astype(str).str.strip()
        
        # Convert eNodeBId to numeric and drop invalid
        df['enodeb_id'] = pd.to_numeric(df['enodeb_id'], errors='coerce')
        
        # Drop rows with invalid eNodeBId
        initial_count = len(df)
        df = df.dropna(subset=['enodeb_id'])
        dropped_count = initial_count - len(df)
        
        if dropped_count > 0:
            print(f"Warning: Dropped {dropped_count} rows with invalid eNodeBId")
        
        df['enodeb_id'] = df['enodeb_id'].astype(int)
        
        return df

    def save_output(self, df: pd.DataFrame, output_file: str) -> None:
        """Save output ke CSV"""
        df.to_csv(output_file, index=False)


class KQIProcessor(IKQIProcessor):
    """Concrete implementation of KQI processing"""
    
    def process(self, kqiraw: pd.DataFrame, sourceraw: pd.DataFrame) -> dict:
        """Main processing pipeline"""
        # Kolom sudah di-rename di DataRepository
        
        # Step 5: Process konversi Network ID
        kqiraw = self._convert_network_id(kqiraw)
        
        # Step 7: Convert kolom waktu
        kqiraw = self._convert_time_column(kqiraw)
        
        # Step 8: Mapping dengan sourceraw
        kqiraw = self._map_tower_data(kqiraw, sourceraw)
        
        # Pisahkan data mapped dan unmapped
        mapped_data = kqiraw[kqiraw['tower_id'].notna()].copy()
        unmapped_data = kqiraw[kqiraw['tower_id'].isna()].copy()
        
        # Step 9: Kalkulasi & Agregasi
        result_mapped = self._aggregate_daily(mapped_data)
        result_unmapped = self._aggregate_unmapped(unmapped_data)
        
        return {"mapped": result_mapped, "unmapped": result_unmapped}
    
    def _convert_network_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert cgisai ke MCC, MNC, eNodeB ID, Cell ID, dan Operator"""
        network_ids = df['cgisai'].apply(NetworkIdentifier.from_cgisai)
        
        df['mcc'] = network_ids.apply(lambda x: x.mcc)
        df['mnc'] = network_ids.apply(lambda x: x.mnc)
        df['plmn'] = network_ids.apply(lambda x: x.plmn)
        df['enodeb_id'] = network_ids.apply(lambda x: x.enodeb_id)
        df['cell_id'] = network_ids.apply(lambda x: x.cell_id)
        df['operator'] = network_ids.apply(lambda x: x.operator)
        
        return df
    
    def _convert_time_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert timecolumn ke Date dan Time"""
        # Handle scientific notation (2.0251E+11)
        df['timecolumn'] = df['timecolumn'].apply(lambda x: f"{int(float(x)):012d}")
        df['datetime'] = pd.to_datetime(df['timecolumn'], format='%Y%m%d%H%M')
        df['Date'] = df['datetime'].dt.strftime('%m/%d/%Y')
        df['Time'] = df['datetime'].dt.strftime('%H:%M')
        return df
    
    def _map_tower_data(self, df: pd.DataFrame, sourceraw: pd.DataFrame) -> pd.DataFrame:
        """Mapping eNodeB ID ke tower data"""
        df = df.merge(
            sourceraw,
            on='enodeb_id',
            how='left'
        )
        return df
    
    def _aggregate_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agregasi data per hari per operator per tower (MAPPED)"""
        if df.empty:
            return pd.DataFrame()
        
        # STEP 1: SUM by eNodeB first
        groupby_enodeb = ['operator', 'Date', 'enodeb_id']
        
        agg_dict_enodeb = {
            'tcp_rtt': 'sum',
            'tcp_rtt_good_count': 'sum',
            'tcp_rtt_step1': 'sum',
            'tcp_rtt_step1_good_count': 'sum',
            'server_probe_ul_lost_pkt': 'sum',
            'server_probe_dw_lost_pkt': 'sum',
            'user_probe_ul_lost_pkt': 'sum',
            'user_probe_dw_lost_pkt': 'sum',
            'tcp_ul_packages_withpl': 'sum',
            'tcp_dl_packages_withpl': 'sum',
            'tower_id': 'first',
            'swe_l5': 'first'
        }
        
        df_enodeb = df.groupby(groupby_enodeb, dropna=False).agg(agg_dict_enodeb).reset_index()
        
        # STEP 2: SUM by tower
        groupby_tower = ['operator', 'Date', 'tower_id', 'swe_l5']
        
        agg_dict_tower = {
            'tcp_rtt': 'sum',
            'tcp_rtt_good_count': 'sum',
            'tcp_rtt_step1': 'sum',
            'tcp_rtt_step1_good_count': 'sum',
            'server_probe_ul_lost_pkt': 'sum',
            'tcp_ul_packages_withpl': 'sum',
            'server_probe_dw_lost_pkt': 'sum',
            'tcp_dl_packages_withpl': 'sum',
            'user_probe_ul_lost_pkt': 'sum',
            'user_probe_dw_lost_pkt': 'sum'
        }
        
        result = df_enodeb.groupby(groupby_tower, dropna=False).agg(agg_dict_tower).reset_index()
        
        # STEP 3: Calculate metrics
        result['E2E Delay(ms)'] = np.where(
            result['tcp_rtt_good_count'] > 0,
            result['tcp_rtt'] / result['tcp_rtt_good_count'],
            0
        )
        
        result['SYN-SYN ACK Delay(ms)'] = np.where(
            result['tcp_rtt_step1_good_count'] > 0,
            result['tcp_rtt_step1'] / result['tcp_rtt_step1_good_count'],
            0
        )
        
        result['SYN ACK-ACK Delay(ms)'] = result['E2E Delay(ms)'] - result['SYN-SYN ACK Delay(ms)']
        
        result['Server Side Downlink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_dl_packages_withpl'] > 0,
            result['server_probe_dw_lost_pkt'] / result['tcp_dl_packages_withpl'] * 100,
            0
        )
        
        result['Server Side Uplink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_ul_packages_withpl'] > 0,
            result['server_probe_ul_lost_pkt'] / result['tcp_ul_packages_withpl'] * 100,
            0
        )
        
        result['Client Side Downlink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_dl_packages_withpl'] > 0,
            result['user_probe_dw_lost_pkt'] / result['tcp_dl_packages_withpl'] * 100,
            0
        )
        
        result['Client Side Uplink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_ul_packages_withpl'] > 0,
            result['user_probe_ul_lost_pkt'] / result['tcp_ul_packages_withpl'] * 100,
            0
        )
        
        result['NATIONAL'] = result['operator'].apply(
            lambda x: f'Indonesia-{x}' if x != 'Unknown' else 'Unknown'
        )
        
        result = result.rename(columns={
            'operator': 'Opr',
            'swe_l5': 'SWE_L5',
            'tower_id': 'SWE_L6',
            'tcp_rtt': 'TCP Connect Delay(ms)',
            'tcp_rtt_good_count': 'TCP Connect RTT Count(times)',
            'tcp_rtt_step1': 'TCP Connect Step1 Delay(ms)',
            'tcp_rtt_step1_good_count': 'TCP Connect RTT Step1 Count(times)',
            'server_probe_ul_lost_pkt': 'Server Side Uplink TCP Packet Losses(Packets)',
            'tcp_ul_packages_withpl': 'TCP Uplink Packets (with Payload)(Packets)',
            'server_probe_dw_lost_pkt': 'Server Side Downlink TCP Packet Losses',
            'tcp_dl_packages_withpl': 'TCP Downlink Packets (with Payload)(Packets)',
            'user_probe_ul_lost_pkt': 'Client Side Uplink TCP Packet Losses(Packets)',
            'user_probe_dw_lost_pkt': 'Client Side Downlink TCP Packet Losses(Packets)'
        })
        
        output_cols = [
            'Opr', 'Date', 'NATIONAL', 'SWE_L5', 'SWE_L6',
            'E2E Delay(ms)',
            'TCP Connect Delay(ms)',
            'TCP Connect RTT Count(times)',
            'SYN-SYN ACK Delay(ms)',
            'TCP Connect Step1 Delay(ms)',
            'TCP Connect RTT Step1 Count(times)',
            'SYN ACK-ACK Delay(ms)',
            'Server Side Uplink TCP Packet Loss Rate(%)',
            'Server Side Uplink TCP Packet Losses(Packets)',
            'TCP Uplink Packets (with Payload)(Packets)',
            'Server Side Downlink TCP Packet Loss Rate(%)',
            'Server Side Downlink TCP Packet Losses',
            'TCP Downlink Packets (with Payload)(Packets)',
            'Client Side Uplink TCP Packet Loss Rate(%)',
            'Client Side Uplink TCP Packet Losses(Packets)',
            'Client Side Downlink TCP Packet Loss Rate(%)',
            'Client Side Downlink TCP Packet Losses(Packets)'
        ]
        
        result['E2E Delay(ms)'] = result['E2E Delay(ms)'].round(0).astype(int)
        result['SYN-SYN ACK Delay(ms)'] = result['SYN-SYN ACK Delay(ms)'].round(0).astype(int)
        result['SYN ACK-ACK Delay(ms)'] = result['SYN ACK-ACK Delay(ms)'].round(0).astype(int)
        
        loss_rate_cols = [col for col in output_cols if '(%)' in col]
        for col in loss_rate_cols:
            if col in result.columns:
                result[col] = result[col].round(2)
        
        return result[output_cols]
    
    def _aggregate_unmapped(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agregasi data per hari per operator per eNodeB (UNMAPPED)"""
        if df.empty:
            return pd.DataFrame()
        
        groupby_cols = ['operator', 'Date', 'enodeb_id', 'plmn']
        
        agg_dict = {
            'tcp_rtt': 'sum',
            'tcp_rtt_good_count': 'sum',
            'tcp_rtt_step1': 'sum',
            'tcp_rtt_step1_good_count': 'sum',
            'server_probe_ul_lost_pkt': 'sum',
            'tcp_ul_packages_withpl': 'sum',
            'server_probe_dw_lost_pkt': 'sum',
            'tcp_dl_packages_withpl': 'sum',
            'user_probe_ul_lost_pkt': 'sum',
            'user_probe_dw_lost_pkt': 'sum'
        }
        
        result = df.groupby(groupby_cols, dropna=False).agg(agg_dict).reset_index()
        
        result['E2E Delay(ms)'] = np.where(
            result['tcp_rtt_good_count'] > 0,
            result['tcp_rtt'] / result['tcp_rtt_good_count'],
            0
        )
        
        result['SYN-SYN ACK Delay(ms)'] = np.where(
            result['tcp_rtt_step1_good_count'] > 0,
            result['tcp_rtt_step1'] / result['tcp_rtt_step1_good_count'],
            0
        )
        
        result['SYN ACK-ACK Delay(ms)'] = result['E2E Delay(ms)'] - result['SYN-SYN ACK Delay(ms)']
        
        result['Server Side Downlink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_dl_packages_withpl'] > 0,
            result['server_probe_dw_lost_pkt'] / result['tcp_dl_packages_withpl'] * 100,
            0
        )
        
        result['Server Side Uplink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_ul_packages_withpl'] > 0,
            result['server_probe_ul_lost_pkt'] / result['tcp_ul_packages_withpl'] * 100,
            0
        )
        
        result['Client Side Downlink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_dl_packages_withpl'] > 0,
            result['user_probe_dw_lost_pkt'] / result['tcp_dl_packages_withpl'] * 100,
            0
        )
        
        result['Client Side Uplink TCP Packet Loss Rate(%)'] = np.where(
            result['tcp_ul_packages_withpl'] > 0,
            result['user_probe_ul_lost_pkt'] / result['tcp_ul_packages_withpl'] * 100,
            0
        )
        
        result['NATIONAL'] = result['operator'].apply(
            lambda x: f'Indonesia-{x}' if x != 'Unknown' else 'Unknown'
        )
        
        result = result.rename(columns={
            'operator': 'Opr',
            'enodeb_id': 'eNodeBID',
            'plmn': 'PLMN',
            'tcp_rtt': 'TCP Connect Delay(ms)',
            'tcp_rtt_good_count': 'TCP Connect RTT Count(times)',
            'tcp_rtt_step1': 'TCP Connect Step1 Delay(ms)',
            'tcp_rtt_step1_good_count': 'TCP Connect RTT Step1 Count(times)',
            'server_probe_ul_lost_pkt': 'Server Side Uplink TCP Packet Losses(Packets)',
            'tcp_ul_packages_withpl': 'TCP Uplink Packets (with Payload)(Packets)',
            'server_probe_dw_lost_pkt': 'Server Side Downlink TCP Packet Losses',
            'tcp_dl_packages_withpl': 'TCP Downlink Packets (with Payload)(Packets)',
            'user_probe_ul_lost_pkt': 'Client Side Uplink TCP Packet Losses(Packets)',
            'user_probe_dw_lost_pkt': 'Client Side Downlink TCP Packet Losses(Packets)'
        })
        
        output_cols = [
            'Opr', 'Date', 'NATIONAL', 'PLMN', 'eNodeBID',
            'E2E Delay(ms)',
            'TCP Connect Delay(ms)',
            'TCP Connect RTT Count(times)',
            'SYN-SYN ACK Delay(ms)',
            'TCP Connect Step1 Delay(ms)',
            'TCP Connect RTT Step1 Count(times)',
            'SYN ACK-ACK Delay(ms)',
            'Server Side Uplink TCP Packet Loss Rate(%)',
            'Server Side Uplink TCP Packet Losses(Packets)',
            'TCP Uplink Packets (with Payload)(Packets)',
            'Server Side Downlink TCP Packet Loss Rate(%)',
            'Server Side Downlink TCP Packet Losses',
            'TCP Downlink Packets (with Payload)(Packets)',
            'Client Side Uplink TCP Packet Loss Rate(%)',
            'Client Side Uplink TCP Packet Losses(Packets)',
            'Client Side Downlink TCP Packet Loss Rate(%)',
            'Client Side Downlink TCP Packet Losses(Packets)'
        ]
        
        result['E2E Delay(ms)'] = result['E2E Delay(ms)'].round(0).astype(int)
        result['SYN-SYN ACK Delay(ms)'] = result['SYN-SYN ACK Delay(ms)'].round(0).astype(int)
        result['SYN ACK-ACK Delay(ms)'] = result['SYN ACK-ACK Delay(ms)'].round(0).astype(int)
        
        loss_rate_cols = [col for col in output_cols if '(%)' in col]
        for col in loss_rate_cols:
            if col in result.columns:
                result[col] = result[col].round(2)
        
        return result[output_cols]