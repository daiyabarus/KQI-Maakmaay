import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os

from core.use_cases import ProcessKQIDataUseCase
from infrastructure.file_operations import FileRepository, DataRepository, KQIProcessor


class ModernKQIGUI:
    """Modern GUI untuk KQI-Maakmaay Application"""
    
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("KQI-Maakmaay")
        self.root.geometry("900x700")
        
        try:
            self.root.iconbitmap("favicon.ico")
        except Exception:
            pass
        
        self.bg_color = "#f0f0f0"
        self.primary_color = "#2196F3"
        self.secondary_color = "#4CAF50"
        self.text_color = "#333333"
        
        self.root.configure(bg=self.bg_color)
        
        self.input_folder = tk.StringVar()
        self.mapping_file = tk.StringVar()
        self.output_folder = tk.StringVar()
        self.is_processing = False
        
        self._create_widgets()
        
        file_repo = FileRepository()
        data_repo = DataRepository()
        processor = KQIProcessor()
        self.use_case = ProcessKQIDataUseCase(file_repo, data_repo, processor)
        self.use_case.set_log_callback(self.log_message)
        self.use_case.set_progress_callback(self.update_progress)

    def _create_widgets(self):
        """Create GUI widgets"""
        
        header_frame = tk.Frame(self.root, bg=self.primary_color, height=100)
        header_frame.pack(fill=tk.X)
        header_frame.pack_propagate(False)
        
        title_label = tk.Label(
            header_frame,
            text="KQI-Maakmaay",
            font=("Segoe UI", 24, "bold"),
            bg=self.primary_color,
            fg="white"
        )
        title_label.pack(pady=10)

        content_frame = tk.Frame(self.root, bg=self.bg_color)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        input_section = tk.LabelFrame(
            content_frame,
            text="Configuration",
            font=("Segoe UI", 12, "bold"),
            bg=self.bg_color,
            fg=self.text_color
        )
        input_section.pack(fill=tk.X, pady=(0, 10))
        
        self._create_folder_input(
            input_section,
            "Input Folder (.csv.gz):",
            self.input_folder,
            self.browse_input_folder,
            0
        )
        
        self._create_folder_input(
            input_section,
            "Mapping File (with header):",
            self.mapping_file,
            self.browse_mapping_file,
            1
        )
        
        self._create_folder_input(
            input_section,
            "Output Folder:",
            self.output_folder,
            self.browse_output_folder,
            2
        )
        
        self.process_btn = tk.Button(
            content_frame,
            text="Start Processing",
            font=("Segoe UI", 12, "bold"),
            bg=self.secondary_color,
            fg="white",
            relief=tk.FLAT,
            cursor="hand2",
            command=self.start_processing,
            height=2
        )
        self.process_btn.pack(fill=tk.X, pady=10)
        
        progress_frame = tk.LabelFrame(
            content_frame,
            text="Progress",
            font=("Segoe UI", 12, "bold"),
            bg=self.bg_color,
            fg=self.text_color
        )
        progress_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        # Progress Label
        self.progress_label = tk.Label(
            progress_frame,
            text="Ready to process...",
            font=("Segoe UI", 10),
            bg=self.bg_color,
            fg=self.text_color,
            anchor="w"
        )
        self.progress_label.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        # Progress Bar
        self.progress = ttk.Progressbar(
            progress_frame,
            mode='determinate',
            maximum=100
        )
        self.progress.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        log_frame = tk.Frame(progress_frame, bg=self.bg_color)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        scrollbar = tk.Scrollbar(log_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.log_text = tk.Text(
            log_frame,
            font=("Consolas", 9),
            bg="white",
            fg=self.text_color,
            yscrollcommand=scrollbar.set,
            wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.log_text.yview)
        
        footer_label = tk.Label(
            self.root,
            text="© 2025 KQI-Maakmaay",
            font=("Segoe UI", 9),
            bg=self.bg_color,
            fg=self.text_color
        )
        footer_label.pack(pady=10)

    def _create_folder_input(self, parent, label_text, variable, command, row):
        """Create folder input row"""
        frame = tk.Frame(parent, bg=self.bg_color)
        frame.pack(fill=tk.X, padx=10, pady=5)
        
        label = tk.Label(
            frame,
            text=label_text,
            font=("Segoe UI", 10),
            bg=self.bg_color,
            fg=self.text_color,
            width=25,
            anchor="w"
        )
        label.pack(side=tk.LEFT)
        
        entry = tk.Entry(
            frame,
            textvariable=variable,
            font=("Segoe UI", 10),
            relief=tk.FLAT,
            bg="white"
        )
        entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        btn = tk.Button(
            frame,
            text="Browse",
            font=("Segoe UI", 9),
            bg=self.primary_color,
            fg="white",
            relief=tk.FLAT,
            cursor="hand2",
            command=command,
            width=10
        )
        btn.pack(side=tk.RIGHT)

    def browse_input_folder(self):
        folder = filedialog.askdirectory(title="Select Input Folder")
        if folder:
            self.input_folder.set(folder)

    def browse_mapping_file(self):
        file = filedialog.askopenfilename(
            title="Select Mapping File (with header)",
            filetypes=[("CSV files", "*.csv"), ("TSV files", "*.tsv"), ("All files", "*.*")]
        )
        if file:
            self.mapping_file.set(file)

    def browse_output_folder(self):
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.output_folder.set(folder)

    def log_message(self, message: str):
        """Log message to text widget - THREAD SAFE"""
        def update_log():
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.see(tk.END)
            self.root.update_idletasks()
        
        self.root.after(0, update_log)

    def update_progress(self, step: int, total_steps: int, message: str = ""):
        """Update progress bar and label - THREAD SAFE"""
        def update_ui():
            progress_percent = (step / total_steps) * 100
            self.progress['value'] = progress_percent
            self.progress_label.config(text=f"Step {step}/{total_steps}: {message}")
            self.root.update_idletasks()
        
        self.root.after(0, update_ui)

    def validate_inputs(self) -> bool:
        """Validate input fields"""
        if not self.input_folder.get():
            messagebox.showerror("Error", "Please select input folder!")
            return False
        
        if not self.mapping_file.get():
            messagebox.showerror("Error", "Please select mapping file!")
            return False
        
        if not self.output_folder.get():
            messagebox.showerror("Error", "Please select output folder!")
            return False
        
        if not os.path.exists(self.input_folder.get()):
            messagebox.showerror("Error", "Input folder does not exist!")
            return False
        
        if not os.path.exists(self.mapping_file.get()):
            messagebox.showerror("Error", "Mapping file does not exist!")
            return False
        
        return True

    def start_processing(self):
        """Start processing in background thread"""
        if not self.validate_inputs():
            return
        
        if self.is_processing:
            messagebox.showwarning("Warning", "Processing is already running!")
            return
        
        # Clear log dan reset UI
        self.log_text.delete(1.0, tk.END)
        self.progress['value'] = 0
        self.progress_label.config(text="Starting processing...")
        
        # Disable button dan set flag
        self.is_processing = True
        self.process_btn.config(state=tk.DISABLED, text="Processing...", bg="#cccccc")
        
        # Enable periodic update check
        self._start_progress_monitor()
        
        # Run in thread
        thread = threading.Thread(target=self.run_processing, daemon=True)
        thread.start()

    def _start_progress_monitor(self):
        """Start periodic UI update to prevent freezing"""
        def monitor():
            if self.is_processing:
                self.root.update_idletasks()
                self.root.after(100, monitor)  # Update setiap 100ms
        
        self.root.after(100, monitor)

    def run_processing(self):
        """Run processing in background"""
        try:
            result = self.use_case.execute(
                input_folder=self.input_folder.get(),
                mapping_file=self.mapping_file.get(),
                output_folder=self.output_folder.get()
            )
            
            # Show success message
            self.root.after(0, lambda: self.on_processing_complete(result))
            
        except Exception as e:
            error_msg = str(e)
            self.root.after(0, lambda em=error_msg: self.on_processing_error(em))

    def on_processing_complete(self, result):
        """Handle processing completion"""
        self.is_processing = False
        self.progress['value'] = 100
        self.progress_label.config(text="Processing completed successfully!")
        self.process_btn.config(state=tk.NORMAL, text="Start Processing", bg=self.secondary_color)
        
        message = "Processing completed successfully!\n\n"
        if result.mapped_file_path:
            message += f"Mapped data: {result.mapped_file_path}\n"
        if result.unmapped_file_path:
            message += f"Unmapped data: {result.unmapped_file_path}"
        
        messagebox.showinfo("Success", message)

    def on_processing_error(self, error_msg: str):
        """Handle processing error"""
        self.is_processing = False
        self.progress['value'] = 0
        self.progress_label.config(text="Processing failed!")
        self.process_btn.config(state=tk.NORMAL, text="Start Processing", bg=self.secondary_color)
        
        self.log_message(f"❌ Error: {error_msg}")
        messagebox.showerror("Error", f"Processing failed:\n{error_msg}")


def main():
    """Main entry point"""
    root = tk.Tk()
    global app
    app = ModernKQIGUI(root)
    root.mainloop()