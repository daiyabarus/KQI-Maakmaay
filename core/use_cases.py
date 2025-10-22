from datetime import datetime
import pandas as pd

from .repositories import IFileRepository, IDataRepository, IKQIProcessor
from .entities import ProcessingResult


class ProcessKQIDataUseCase:
    """Use Case untuk memproses KQI data"""
    
    def __init__(
        self, 
        file_repo: IFileRepository,
        data_repo: IDataRepository, 
        processor: IKQIProcessor
    ):
        self.file_repo = file_repo
        self.data_repo = data_repo
        self.processor = processor
        self.log_callback = None
        self.progress_callback = None

    def set_log_callback(self, callback):
        """Set callback untuk logging"""
        self.log_callback = callback

    def set_progress_callback(self, callback):
        """Set callback untuk progress update"""
        self.progress_callback = callback

    def log(self, message: str):
        """Log message dengan callback"""
        if self.log_callback:
            self.log_callback(message)

    def update_progress(self, step: int, total_steps: int, message: str = ""):
        """Update progress dengan callback"""
        if self.progress_callback:
            self.progress_callback(step, total_steps, message)

    def execute(
        self, 
        input_folder: str, 
        mapping_file: str, 
        output_folder: str
    ) -> ProcessingResult:
        """Execute complete processing pipeline"""
        
        total_steps = 10
        
        self.log("=" * 80)
        self.log("KQI-MAAKMAAY")
        self.log("=" * 80)

        extract_folder = f"{output_folder}/_temp_extracted"
        
        try:
            # Step 1: List files
            self.update_progress(1, total_steps, "Listing .csv.gz files...")
            self.log("\n[Step 1/9] Listing .csv.gz files...")
            gz_files = self.file_repo.list_gz_files(input_folder)
            self.log(f"Found {len(gz_files)} files")

            if not gz_files:
                raise Exception("No .csv.gz files found!")

            # Step 2: Extract files
            self.update_progress(2, total_steps, "Extracting files...")
            self.log("\n[Step 2/9] Extracting .csv.gz files...")
            self.file_repo.create_directory(extract_folder)
            csv_files = self.file_repo.extract_gz_files(gz_files, extract_folder)
            self.log(f"Extracted {len(csv_files)} files")

            # Step 3: Load and merge CSV files
            self.update_progress(3, total_steps, "Loading CSV files...")
            self.log("\n[Step 3/9] Loading CSV files (NO HEADER)...")
            kqiraw = self.data_repo.load_csv_files(csv_files, delimiter="|")
            self.log(f"Total records loaded: {len(kqiraw):,}")

            # Step 4: Load mapping file
            self.update_progress(4, total_steps, "Loading mapping file...")
            self.log("\n[Step 4/9] Loading mapping file (WITH HEADER)...")
            sourceraw = self.data_repo.load_mapping_file(mapping_file)
            self.log(f"Mapping records loaded: {len(sourceraw):,}")

            # Step 5-9: Process data
            self.update_progress(5, total_steps, "Processing data...")
            self.log("\n[Step 5-9/9] Processing data...")
            self.log("  → AGG")
            self.log("  → SUM")
            self.log("  → SAVING OUTPUTS")
            
            results = self.processor.process(kqiraw, sourceraw)
            
            result_mapped = results.get("mapped", pd.DataFrame())
            result_unmapped = results.get("unmapped", pd.DataFrame())
            
            self.log(f"Mapped records: {len(result_mapped):,}")
            self.log(f"Unmapped records: {len(result_unmapped):,}")

            # Save outputs
            self.update_progress(9, total_steps, "Saving outputs...")
            self.log("\n[Final] Saving outputs...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            mapped_file = f"{output_folder}/KQI_{timestamp}.csv"
            unmapped_file = f"{output_folder}/kqi_unmapped_{timestamp}.csv"

            if not result_mapped.empty:
                self.data_repo.save_output(result_mapped, mapped_file)
                self.log(f"✓ Mapped data saved: {mapped_file}")

            if not result_unmapped.empty:
                self.data_repo.save_output(result_unmapped, unmapped_file)
                self.log(f"✓ Unmapped data saved: {unmapped_file}")

            self.update_progress(10, total_steps, "Cleaning up...")
            self.log("\n" + "=" * 80)
            self.log("PROCESSING COMPLETE!")
            self.log("=" * 80)

            return ProcessingResult(
                mapped_data=result_mapped,
                unmapped_data=result_unmapped,
                mapped_file_path=mapped_file if not result_mapped.empty else "",
                unmapped_file_path=unmapped_file if not result_unmapped.empty else ""
            )

        except Exception as e:
            self.log(f"❌ Error during processing: {str(e)}")
            raise
        finally:
            # Always cleanup extracted files
            self._cleanup_extracted_files(extract_folder)

    def _cleanup_extracted_files(self, extract_folder: str):
        """Cleanup extracted files after processing"""
        try:
            self.log("\n[Cleanup] Removing temporary files...")
            self.file_repo.remove_directory(extract_folder)
            self.log(f"✓ Temporary folder removed: {extract_folder}")
        except Exception as e:
            self.log(f"⚠️  Warning: Could not remove temporary files: {str(e)}")