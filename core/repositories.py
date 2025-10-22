from abc import ABC, abstractmethod
from typing import List, Dict
import pandas as pd

class IFileRepository(ABC):
    """Interface untuk file operations"""
    
    @abstractmethod
    def list_gz_files(self, folder_path: str) -> List[str]:
        pass

    @abstractmethod
    def extract_gz_files(self, gz_files: List[str], output_folder: str) -> List[str]:
        pass

    @abstractmethod
    def create_directory(self, directory_path: str) -> None:
        pass

    @abstractmethod
    def remove_directory(self, directory_path: str) -> None:
        pass


class IDataRepository(ABC):
    """Interface untuk data operations"""
    
    @abstractmethod
    def load_csv_files(self, csv_files: List[str], delimiter: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def load_mapping_file(self, mapping_file: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def save_output(self, df: pd.DataFrame, output_file: str) -> None:
        pass


class IKQIProcessor(ABC):
    """Interface untuk KQI processing"""
    
    @abstractmethod
    def process(self, kqiraw: pd.DataFrame, sourceraw: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        pass