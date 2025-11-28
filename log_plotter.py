import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Set, Tuple
import re
import math
import difflib

# Handle missing log_parser gracefully
try:
    from log_parser import parse_log_file, convert_to_polars
except ImportError:
    print("Warning: log_parser module not found. Some functionality may be limited.")
    def parse_log_file():
        messagebox.showerror("Error", "log_parser module not available!")
        return {}, ""
    def convert_to_polars(dfs):
        return {}

class Config:
    """Centralized configuration for all application parameters"""
    
    # ============ Application Settings ============
    APP_TITLE = "CC log analyzer v0.9t"
    WINDOW_SIZE = "1400x800"
    LEFT_PANEL_WIDTH = 300
    
    # ============ UI Text Labels ============
    # Tab Names
    TAB_PLOTTER = "  Plotter  "
    TAB_TABLE1 = "  Table-1  "
    TAB_TABLE2 = "  Table-2  "
    TAB_SEARCH_RESULTS = "  Search Results  "
    
    # Button Labels
    BTN_LOAD_LOG = "Load Log File"
    BTN_PLOT_SELECTED = "Plot Selected"
    BTN_CLEAR_PLOT = "Clear Plot"
    BTN_SEARCH = "Search"
    BTN_EXPORT_CSV = "Export to CSV"
    BTN_LOAD_ALL_ROWS = "Load All Rows"
    BTN_SHOW_HIDE_COLS = "Show/Hide Columns"
    BTN_APPLY_CLOSE = "Apply & Close"
    BTN_CANCEL = "Cancel"
    BTN_HIDE_ALL = "Hide All"
    BTN_UNHIDE_ALL = "Unhide All"
    BTN_SELECT_ALL = "Select All"
    BTN_DESELECT_ALL = "Deselect All"
    BTN_REFRESH_PLOT = "Refresh Plot"
    BTN_COPY_CLIPBOARD = "Copy to Clipboard"
    BTN_CLOSE = "Close"
    
    # Frame/Section Labels
    LABEL_LOAD_ANALYZE = "Load And Start Analyzing..."
    LABEL_Y_AXIS_SCALE = "Y-axis scale:"
    LABEL_LEFT_AXIS = "Left Y-axis"
    LABEL_RIGHT_AXIS = "Right Y-axis"
    LABEL_LINEAR = "Linear"
    LABEL_LOG = "Log"
    LABEL_NO_DATA = "No data loaded"
    LABEL_NO_SEARCH = "No search results"
    LABEL_SEARCH_PLACEHOLDER = "🔍 No search performed yet"
    
    # Tree View Headers
    TREE_HEADER_VARIABLES = "CC Variables"
    TREE_HEADER_TYPE = "Type"
    TREE_HEADER_COUNT = "Count"
    
    # Context Menu Labels
    MENU_VIEW_FULL_CONTENT = "View Full Content"
    MENU_COPY_CELL_VALUE = "Copy Cell Value"
    MENU_HIDE_COLUMN = "Hide '{}' Column"
    MENU_CANNOT_HIDE_TIMESTAMP = "Cannot hide timestamp column"
    MENU_SHOW_HIDDEN_COLUMNS = "Show Hidden Columns"
    
    # Dialog Titles
    DIALOG_SEARCH_TITLE = "Search in {}"
    DIALOG_COLUMN_VISIBILITY = "Column Visibility - {}"
    DIALOG_FULL_CONTENT = "Full Content - {}"
    DIALOG_LARGE_DATASET = "Large Dataset Warning"
    DIALOG_SEARCH_COMPLETE = "Search Complete"
    DIALOG_SUCCESS = "Success"
    DIALOG_ERROR = "Error"
    DIALOG_WARNING = "Warning"
    DIALOG_EXPORT_ERROR = "Export Error"
    DIALOG_SEARCH_ERROR = "Search Error"
    DIALOG_TABLE_ERROR = "Table Error"
    DIALOG_COMPLETE = "Complete"
    
    # Search Dialog Labels
    SEARCH_LABEL_TITLE = "Search in: {}"
    SEARCH_LABEL_TERM = "Search term:"
    SEARCH_LABEL_FUZZY = "Approximate string search (similarity ≥ 80%)"
    SEARCH_LABEL_CASE = "Case sensitive"
    SEARCH_LABEL_COLUMNS = "Columns to search (excluding timestamp)"
    SEARCH_TITLE_OPTIONS = "Search Options"
    
    # Column Dialog Labels
    COL_DIALOG_TITLE = "Column Visibility Settings"
    COL_DIALOG_SUBTITLE = "Check columns to show, uncheck to hide (timestamp always visible)"
    COL_DIALOG_STATUS = "Currently: {} visible, {} hidden"
    
    # Messages
    MSG_NO_DATA_TABLE = "No data loaded in this table!"
    MSG_ENTER_SEARCH_TERM = "Please enter a search term!"
    MSG_SELECT_COLUMN = "Please select at least one column to search!"
    MSG_FOUND_MATCHES = "Found {} matching rows out of {} total rows."
    MSG_NO_MATCHES = "No matches found for '{}' in the selected columns."
    MSG_LOAD_SUCCESS = "Log file loaded successfully!"
    MSG_LOAD_FAILED = "Failed to load log file: {}"
    MSG_NO_LOG_FILE = "Please load a log file first!"
    MSG_SELECT_VARIABLES = "Please select variables to plot!"
    MSG_NO_TABLE_DATA = "No table data to export!\nPlease double-click a DataFrame in the variable tree first."
    MSG_EXPORT_SUCCESS = "Data exported successfully to:\n{}"
    MSG_EXPORT_FAILED = "Failed to export data:\n{}"
    MSG_CANNOT_HIDE_TIMESTAMP = "Cannot hide timestamp column!"
    MSG_LOAD_ALL_WARNING = "You're about to load {:,} rows.\nThis might take some time and use significant memory.\n\nContinue?"
    MSG_ALL_ROWS_LOADED = "All {:,} rows loaded successfully!"
    MSG_INVALID_COLUMN = "Invalid column"
    MSG_ROW_OUT_OF_BOUNDS = "Row index {} is out of bounds"
    MSG_NO_CELL_SELECTED = "No cell selected"
    MSG_INVALID_COLUMN_SELECTED = "Invalid column selected"
    MSG_COULD_NOT_RETRIEVE = "Could not retrieve cell value: {}"
    MSG_COULD_NOT_COPY = "Could not copy cell value: {}"
    MSG_COULD_NOT_PROCESS = "Could not process cell click: {}"
    MSG_TABLE_ERROR = "Error displaying table:\n{}"
    MSG_LOAD_ALL_ROWS_ERROR = "Failed to load all rows:\n{}"
    
    # Info Labels
    INFO_COMPLETE_DATASET = " (Complete dataset)"
    INFO_SEARCH_RESULTS = " (Search results)"
    INFO_NUMERICAL_ONLY = " (Numerical data only from the complete dataset)"
    INFO_HIDDEN_COLS = " ({} hidden)"
    INFO_USE_HSCROLL = " (Use horizontal scroll)"
    INFO_LOADED_ROWS = "  |  Loaded {}/{} rows (auto-loading...)"
    INFO_ALL_LOADED = "  |  All {} rows loaded"
    INFO_ALL_ROWS_LOADED_FULL = "ALL {:,} rows loaded"
    INFO_ROWS_COLS = "{} rows, {} columns"
    
    # Search Result Info Template
    SEARCH_INFO_TEMPLATE = ("🔍 Search: \"{}\" in {}\n"
                           "   Columns: {} ({} of {} columns)\n"
                           "   {}, {}, Found {} matching rows")
    SEARCH_FUZZY_ENABLED = "Fuzzy match ≥80%"
    SEARCH_EXACT_MATCH = "Exact match"
    SEARCH_CASE_SENSITIVE = "Case sensitive"
    SEARCH_CASE_INSENSITIVE = "Case insensitive"
    
    # Progress Messages
    PROGRESS_SEARCHING = "Searching {:,} rows..."
    PROGRESS_LOADING_ROWS = "Loading all rows..."
    PROGRESS_SEARCH_ROW = "Searching row {:,} of {:,}"
    PROGRESS_LOAD_ROW = "Loading rows {:,} to {:,}"
    
    # ============ Table & Performance Settings ============
    TABLE_BATCH_SIZE = 500
    SCROLL_SPEED = 5
    LARGE_DATASET_WARNING = 20000
    PROGRESS_THRESHOLD = 5000
    LOAD_ALL_CHUNK_SIZE = 1000
    
    # ============ Search Settings ============
    FUZZY_SEARCH_THRESHOLD = 0.80
    FUZZY_SEARCH_SLIDING_THRESHOLD = 0.88
    FUZZY_SEARCH_WORD_THRESHOLD = 0.85
    FUZZY_SEARCH_MIN_LENGTH = 3
    FUZZY_SEARCH_CHAR_COVERAGE = 0.6
    FUZZY_SEARCH_LENGTH_TOLERANCE = 2
    SEARCH_DIALOG_SIZE = "600x500"
    MAX_SEARCH_RESULTS = 10000
    SEARCH_PROGRESS_THRESHOLD = 1000
    
    # ============ Column Width Settings ============
    TIMESTAMP_WIDTH_NORMAL = 180
    TIMESTAMP_WIDTH_MANY_COLS = 260
    TIMESTAMP_WIDTH_EXTENDED = 250
    MIN_COLUMN_WIDTH = 180
    MAX_COLUMN_WIDTH = 400
    HEADER_PADDING = 25
    CHAR_WIDTH_MULTIPLIER = 9
    MANY_COLUMNS_THRESHOLD = 15
    
    # ============ Content Truncation ============
    LONG_VALUE_THRESHOLD = 100
    TRUNCATE_LENGTH = 97
    TRUNCATE_SUFFIX = "..."
    DISPLAY_TRUNCATE_THRESHOLD = 40
    DISPLAY_TRUNCATE_LENGTH = 37
    
    # ============ Timing & Delays (milliseconds) ============
    SCROLL_CHECK_DELAY = 100
    SCROLL_MONITOR_INTERVAL = 200
    SCROLLBAR_SETTLE_DELAY = 50
    
    # ============ Scroll Detection Thresholds ============
    SCROLL_BOTTOM_THRESHOLD = 0.9
    VISIBLE_ITEMS_THRESHOLD = 0.9
    SCROLL_POSITION_SENSITIVITY = 0.01
    
    # ============ Plot Settings ============
    PLOT_FIGURE_SIZE = (10, 6)
    MARKER_SIZE = 8
    LINE_WIDTH = 1.5
    ALPHA = 0.8
    LEGEND_FONTSIZE = 8
    
    # ============ Color Settings ============
    LEFT_AXIS_COLOR = "blue"
    RIGHT_AXIS_COLOR = "red"
    PLOTTED_LEFT_BG = "lightblue"
    PLOTTED_LEFT_FG = "darkblue"
    PLOTTED_RIGHT_BG = "lightpink"
    PLOTTED_RIGHT_FG = "darkred"
    GRID_ALPHA = 0.3
    SEARCH_HIGHLIGHT_BG = "lightyellow"
    
    # ============ Color Map Settings ============
    LEFT_COLORMAP_SIZE = 10
    RIGHT_COLORMAP_SIZE = 8
    
    # ============ File Settings ============
    CSV_EXTENSIONS = [("CSV files", "*.csv"), ("All files", "*.*")]
    DEFAULT_EXPORT_NAME = "exported_data"
    
    # ============ Dialog Settings ============
    PROGRESS_DIALOG_SIZE = "400x120"
    COLUMN_DIALOG_SIZE = "650x550"
    CELL_CONTENT_DIALOG_SIZE = "800x600"
    
    # ============ Sample Sizes for Analysis ============
    COLUMN_SAMPLE_SIZE_SMALL = 10
    COLUMN_SAMPLE_SIZE_LARGE = 20
    
    # ============ Axis Margin Settings ============
    AXIS_MARGIN_FACTOR = 0.05
    FALLBACK_MARGIN_FACTOR = 0.1
    
    # ============ Progress Bar Settings ============
    PROGRESS_BAR_LENGTH = 300
    
    # ============ Legend Settings ============
    LEGEND_BBOX = (1.05, 1)
    LEGEND_LOCATION = 'upper left'
    LEGEND_BORDERPAD = 0.5
    LEGEND_COLUMNSPACING = 1.0
    LEGEND_HANDLELENGTH = 1.5
    LEGEND_HANDLETEXTPAD = 0.5

class TableState:
    """Manage state for individual table tabs"""
    def __init__(self, state_id: str):
        self.state_id = state_id
        self.current_table_df: Optional[pd.DataFrame] = None
        self.current_table_name: str = ""
        self.hidden_columns: Set[str] = set()
        self.table_batch_size = Config.TABLE_BATCH_SIZE
        self.table_current_batch = 0
        self.table_total_batches = 0
        self.all_rows_loaded = False
        self.last_scroll_position = 0.0
        
    def reset_for_new_dataframe(self):
        """Reset state when loading a new dataframe"""
        self.hidden_columns.clear()
        self.all_rows_loaded = False
        self.table_current_batch = 0
        self.table_total_batches = 0
        self.last_scroll_position = 0.0

class SearchResult:
    """Container for search results"""
    def __init__(self, source_df_name: str, search_term: str, searched_columns: List[str], 
                 result_df: pd.DataFrame, fuzzy_enabled: bool, case_sensitive: bool):
        self.source_df_name = source_df_name
        self.search_term = search_term
        self.searched_columns = searched_columns
        self.result_df = result_df.copy()
        self.fuzzy_enabled = fuzzy_enabled
        self.case_sensitive = case_sensitive
        self.match_count = len(result_df)

class LogDataPlotter:
    def __init__(self):
        self.root = tk.Tk()
        self.root.app_title = Config.APP_TITLE
        self.root.title(self.root.app_title)
        self.root.geometry(Config.WINDOW_SIZE)

        # Bind cleanup to window close event
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Data storage
        self.current_log_filename: str = ""
        self.pandas_dfs: Dict[str, pd.DataFrame] = {}
        self.polars_dfs = {}
        self.selected_variables: List[str] = []
        self.plotted_variables: Set[str] = set()
        self.plotted_variables_right: Set[str] = set()

        # Table states
        self.table1_state = TableState("table1")
        self.table2_state = TableState("table2")
        self.search_state = TableState("search")
        self.current_search_result: Optional[SearchResult] = None

        # UI references for table tabs
        self.table_tabs = {}
        self.active_table_state = self.table1_state

        # Initialize timer attributes
        self.table1_scroll_timer = None
        self.table2_scroll_timer = None
        self.search_scroll_timer = None

        # Column management dialog state
        self.column_vars = {}
        self.current_column_dialog = None

        self._setup_icon()
        self.setup_gui()
        
        # Setup keyboard shortcuts
        self._setup_keyboard_shortcuts()

    def _setup_icon(self):
        """Setup application icon"""
        try:
            icon_data = '''iVBORw0KGgoAAAANSUhEUgAAAQAAAAEAEAYAAAAM4nQlAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAACTKSURBVHhe7d0JkB31feDxf/e759JcGtCNCoROhMtSjFZCGHlxNmu2gDUsZk2MVyYVpHLismPK7NoxEvbaFWqJy4BtICHEVpksAS9LKBPbmF0hCQkjK1DWBTKHJHShuSTN8e7u3vnR/d9RVBpJ8/pdPf39VKla/X8983+vp1//f/0/FQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAdczwthPWipt+fdNn7//0p81/Y9jRFffea34t8k+RfztvnrEqssX840TCGOEdCozbSy8NDz/3nLdTguuua2y86SZvBwgZZ4S7tUYoZdv5vGyLxWy2UJD9YtG2Pzy0Mv7E+Ev1/PCwOun8yvn5gQPqQeNW87+89JJ1rfq0mv/44zt++umv/eOvd+/2jp5wJlzh9+8v++ff3/7nLS1DX0m8GM1u2xZ5JrY4ai9cSEGPSiAAACqnWMxk8nmlCoWhoVxuNGCoNOenKqUilmXc63xCXfroo4P/0f4n562/+Iu9t976mWd+Ju9oYjC9beDpgn/4mtTfxlYfOBD9WfxKCn4ACK5oNJWKx5WKx1tbU6mRJ9Yq3c+NP1YZZUUi6j3jBfX7L36x+b3IPxu3/+IXC55++h//0y3yjiaGCRMADM2Nvxn71auvRvZHb4j8sK3NSwYABFwkEotFoxIQNDYmEl5iNf1vdYP6n5/4RHNr9C/N//vXf+2lBl7gAwDdxh8ZjrdFVi5Y4CUDACaYWKyhQZ6/TTMaNWtQejmznQedf7d27R/84Gf/cPNtCxd6yYEV+ABAd+6jqh8AwiESSaViMW+ninTTgHmD8cXIR+6800sOrOAHAF+L/Sr6H+bN83YBABOcbhKomT715+rxT37S2wuswAcAxn3mOvXCxOmUAQA4N9OMRGpa5/tRZau/mTXL2wuswFebr1q1Zcudd1ZnaMi5zJlj26dOKbV8ebF47JhSV1xh2729SrW3O44MX+nqsu102jt4RHe3aTY0KNXfbxjSqWXXLtPs7FRq27ZodMoUpd5+2zQnTfIORt1iGCCCYKLen9Lp7u7BQW+nBl577eabn346uM3PBADjpP/UK1da1pEjSq1enc+/+aZSM2bY9tCQ+1o5HDpkmk1NSj
                            3xRDwuXRtfeSUSmTpVxsF6B6AuEACgnoTt/kQA4E/gmwCqZcoUxxkeVurBB7PZzZuVuvfebPa3vy3/F0vTv3fdumx2+3alvv/9TGbLFqUuvthxTo/UAYD7E0pBAHAeixbZdl+fUg8/nMnIF2vBAsvq7/derKKFC9338aMfZTIvv6zUlVdallThAQgv7k/wgwBgDIsXuxf0Aw9ks1u3KtXa6raV1VpLi+PIRJT335/Lbds2+j4BhAf3J5QDAcAZdBWWVKG99prMPOU4FV2MokT6fa1f777PqVNtW6oAAUxc3J9QTgQAHt2N4+tfz+V27HAj6iAs+aAj7nvuyeVef330cwCYOLg/oRIIADwrVxaL0mu2Vm1ofuk2uBUrisWjR71EABMC9ydUQugDAB2Rrl5dKMhwGf/cgTBSUReJyHrWfX0yZaVlHT8u0xXprU7Xx4lyRMd62A+A4OP+hEoKfIWM33kA9AQZjzySyWzc6CWWQFq85AviOCdOyBfGcQqF8SxWYRixmHwKw2hrKxZla5p+xtS+/no02tWllAzVqcWc2WFxzTXuk9l4FYvyF1bqj/6ooeGGG7xE4AwT9f60Zk0qtWqVUu+8429CIeYB8Cf0AcDnP5/Pv/WWUp/7XKEg2/Fz87bt/n6Zm3q8X6wzGYZ0n1FK5t2SL5rw80VDfTp+3J1p7fbbU6k//EMvETjDRL0/bdgQj8sKLhs2xGJ+VnIhAPDHx6UwMegpMUvlOJlMKRH1WBxHng1lm04H97LC+ezdK89S3g4whol6f1q8uFhkeGDthT4A6Ox0nGzW2ymBfMHK8cU6k/7iYmJ6+eVYbPp0bwcYw0S9P3V0KJXJeDuomdAHAB0dtu3vC+ZGxOVWqd+L2tq7NxJpb5dFVSIRWVQFOJeJen/q7PT3uVAeoQ8AHIdiFpU3OGgY0rv6r/4qHl+yRK477wXgHCbq/Yn7bn0IfQAgy10mk95OCXSnmHKT31uPM3xhfI4cMc3GRqW+8pVk8uqrlTp61N0HLsREvT/19fn7XCiP0AcAfi9E+elKFNSG0dBAABA83d2GkUop9fd/H4vNn6/UXXelUp/4hFIHDphmS4t3EHCBJur9SToAyvcEtRX4aph6GgboTpxRKPip3NLjbU2zo8PPMJtNm6JRWZ9706ZIZNo0LxFll8nIs5AU/O6wvoMHDaO52XsR8Gmi3p8YBlgfQh8AVGKiDT3edjyXxehEG62t7kQbkUjpn6p8E20AqB3uT+dGAOBP6JsA9AV46JBpNjV5iSXQM2PpyFgqfC3L/eK
                            4VWX6InG3Ol1yd493f87vF0s/gVLwA8HH/QmVFNjIRfNbA6CtXGlZskjFunXZ7PbtXmIArVuXTF51lVJbtzLMDBeuq0tGdiu1dm0ut2uXUkuW2HZ3t/fiiDfeMM3Jk5V6/PF4fOFC/wXSmcKe//lwfzo7agD8CX0NgPbKK5GItJnv2SOxrpcYILt3u++bgh/joQu+xx5zq5h1QdPQICO9R7crVljWsWNKPfRQNrt58+jP+RX2/C8U9ydUAgGAR9chfPe7iYSM0z550jASCTetng0MuOPL778/mZT3DYyHfuJtbr6w9eX1cWvW5PO7d3uJPoQ9/wvF/QmVQABwBr1Iy7e+lUx+7GOjq7bVG/2+1q1LJKRK7dgxGZjjvQhcoDOrui/U0qWycKy340PY8x8v7k8oJwKAMezc6VZZ3X13MrlihVInTtRHxK0j6nvuSSSWL1dq165IJIhVgqgPuop7vEr9uTOFPf9ScX9CORAAnIduu/rSl1Kpa66pXRucfh9r1yaT116r1O9+F4l0dnovAggl7k/wgwDgAkkVlkzh+uUvp1IrVyq1fn0iIVVwlZr4Rf9eXYWm89VVgACgcX9CKRgGWCaXXeZO2LF8udtbWK93LSu/yapXkyfb9um9hnt6TFOmwuzvV0qm+ty5MxqVCFqvEsc4WVTDSy8NDz/3nLdTguuua2y86SZvpwRhz79aJur9iWGA/hAAACFGABCOAGCiIgDwhyYAAABCiAAAAIAQIgAAACCE6AMATGDRaH+/TMiSSOzdK8vBxuNHjsi+acqIbVncRaZs8Q7GuDlONCp3H9tuaZFtPj99uiyyk8vNny/zBBSL7e1+1s3HudEHwB9qAIAJRYofpZqaNm+WiWFaW59+Wnpzp1K7dskysJGIGxBQ8JeHPo/6vKZSO3dKoKXPe2Pjli3uBD2WxflGvSEAACYEt+CfNOmFF2TYVjK5Z48U+KOzyKO63POeSu3eLX8H/XchEEA9IQAAJoCmpq1bZQrWWOzIkUjES0TdkL+L1BA0Nb36qtQQAPWAAAAIMN3Gn0y6bfyob8nk7t3yd9JNBkAtcQkCAaY791HVHxTu3ymZfPNNt4kGqB0CACDA4vHDh3mSDJ5Y7PBhmmpQa9w6gAAzzaEhAoDgiUQGB+kMiFoL/CVYL/MARCLujbihYetWqZKNxw8dkgjfMAqFSn7RHScWk0+vxx+n08uWFQrS17i1tZ7GH4f9/HR1OY4strJ2bS63a5dSS5bYdne3/3Xlb7ttw4bbb/d2fIhGm5tbWpS66KJVq66/
                            XoavXXLJZZdJgBEf4R0UQradHyHXzcGD776rVHf3li0vvijXU39/b693kA9PPXXHHU8+6e2UIJ02DGlKeOMN05w8WanHH4/HFy5U6tAh02xq8g6awJgHwB+eHXzSBVtr6zPPyDCfROK99+QLWemCTdP5JBL797vDjZ59VsYf6/dVa2E/P7rgf+yxTGbjRqVWrrSso0f9F/zlogv+2bPvuOPP/kyp5ubLL1+wgIJf0+ehqWnOnPnzlbrkks9+9k//dPS81Zq+jlascFf5e+ihbHbz5tHrDjgXAgCf9BOtYWSz9RAHmmYuJ9vGxm3b3M5htRX286Of+JubHUeeJOuNfuKPRFIjvESMyTSTSTlPXV3XXvupT3mJdURfZ2vW5PO7d3uJwBgIAHzSVdn1Jharj/cV9vOjq/rrla7qx/g0Nc2eXc/nbelSyzp+3NsBxkAA4JPfquxiMZtNp5UaHDx69OBBpQYGjhyRbbGYyUh6qQwjn6+HJ27OT32jqr80ct7cKX7rkzOC6xvnQwBQY+l0X19Pj3Q2Ko6QL65lyTaT6e+X9LAL+vnRnbOAatqxIxrt6vJ2gDEQANSYLtDOZNtnTw+boJ8f3StbBn3xpI1KkzUe5Tp79NF4fNEiLxEYAwEAUEF6ONZdd6VSq1YptXlzNDpt2ujwLcAPfR1t2uReV/o66+kxDDp14nwC30pU63kAOjsfeaSx0dspwalT778v44vHMmnSzJmXXurtlKC3d+3a4WFvpwY4P5Xl9/zOm3f33d/+trdTglhscLCvT0Z7fPDB738vbeOFgjvOoj7Zdiwmbffp9JQpc+cqVSg0NbW3ey+W4K23Hnjgm9/0dkoQ9Ouv1pgHwB9qAACULCgFv6bfZ0PD0aP79nmJQEgRAAAoWVAK/jOZZrEYxPcNlBMBAAAAIUQAAABACBEAAAAQQgQAAACEEAEAAAAhxDwAPjHO/dw4Py69PKteHVAvEuR3WeDbbtuw4fbbvZ0S+J0HoK1t796XX/Z2AujEiQULrr3W2ymB33kAnnrqjjuefNLbKYGeCEhPOa1nntQTUE10zAPgDzUAQAXpgv+xxzKZjRuVWrnSso4e9V/wA0JfRytWWNaxY0o99FA2u3nz6HUHnAsBAFBB+olfr9MOVJK+ztasyed37/YSgTEQAAAVpKv6gWpautSyjh/3doAxEAAAwATjjAhuyzSqhQAAqCDdOQuoph07otGuLm8HGAMBAFBBulf24KC7TjtQSQMD7nX26KPx+KJFXiIwBgIAoIL0cCy9Tvvmze667Xr4F
                            uCHvo42bXKvK32d9fQYRirlHQSMgXkAfGKc+7lxfirL7/llHoDazgMQ9Ouv1pgHwB9qAAAACCECAAAAQogAAACAECIAAAAghAgAAAAIIQIAACWz7VgskfB2AsS2o9Egvm+gnAgAAJQsnZ4yZe7c4AQCuuBPp6dOlfcNhBnzAPjEOPdz4/y49PKsenVAvUiQ32WBb7ttw4bbb/d2SuB3HoCw8zsPwFNP3XHHk096OyXQEwHpKaf1zJN6AqqJjnkA/KEGAKggXfA/9lgms3GjUitXWtbRo/4LfkDo62jFCss6dkyphx7KZjdvHr3ugHMhAKgxw4hEzjYlrGmePT1sgn5+9BO/XqcdqCR9na1Zk8/v3u0lAmMgAKixhob2dqm60wWdLthSqY4OVpEL/vnRVf1ANS1dalnHj3s7wBgIAHxynFjMTw+EaDSVamhQqqVl2rRZsySCd7fRaDIp6aVynPpYe47zU99sOz/C28EFs+1cLpv1duqQMyK4LdOoFgIAn/L56dMty9upI/n8zJn10MYc9vOjO2fVq3T64MFzdbLE2Q0PHzjwzjveTh3asSMa7erydoAxEAD4lE4vW1YoyBNBfQyCcpxkUp64h4eXL6+HJ7uwnx/dK3tw0F2nvd50d2/Z8uKL8vfJZuk0dn6Wlc2m03LeXn75F7/wEuvIwIB7nT36aDy+aJGXCIyBAMAny2pttW0ZrnbrrXIDzecvvVSeLKWKuRqDE3U+udxll0m+J06478O2Gxurkf/5hP386OFYep32zZvdddv18K1ay+f7+3t7lXrvvZ/85Ac/kAJk3749e+T85PO5nHdQiOnzMDi4b590qjtw4Cc/+eEPlSoUBgcHBryDakhfR5s2udeVvs56egwjlfIOAsbAPABAgHV0PP649IUwjEKBNt/g0IFpX9+dd0qNAkrDPAD+UAMABJhtNzUR/gaPZfF3Q+0RAAABls/PmFGPnSxxboUCfzfUHgEAEGC53Pz57mgGGgCCwf07ZbP67wbUDgEAEGDFYnu7dLLMZBYupECpf5nMokXyd7Kstjb5uwG1RAAATAB6WGOhMG0aVcv1R8+HkU4vX87oCtQLAgBgQohEpFPZqVPXXy8FTDZ7xRUy/wJNA7XinvdM5oor5Il/cPD662XmQMcxueeibnAxAhOKGwgMDV19tdQI6HkPMpnFiyUg0E0GMkWz9wPwQZ/H0aaYK6+U83zixGc+I+d9ePjqqyUgo+BHPQr80wHzAACle+ml4eHnnvN2SnDddY2NN93k7ZQg7PnDH+YB8IeoFA
                            CAECIAAAAghAgAAAAIIQIAAABCiAAAAIAQIgAAACCECAAAAAghAgAgxNJpw4hGvZ1xGB5WqhxTCYU9f5SmUBgeruWUys4fqP+lPrN+vbcbWAQAQIi98YZpTp7s7YzDjh3RaFeXt+ND2PPH+OiCX7Yy02W16YJ/+w9u/oenb77vPi85sAgAgBB7/PF4fOFCmaveMOJxL/EcBgbc4x59NB5ftMhL9CHs+ePCUPBXRsTbBtbs2V/4wkc/GvyqGKAWdIG2cWM0On26UpMnO44sWiNbmcu+UDAMmcV+27ZodMoUpdavTyavukqpnh7DSKW8X+JD2PPHuVHwVxZrAQAA6goFf3XQBAAAqAsU/NVFAAAAqCkK/togAAAA1AQFf20RAAAAqoqCvz4QAAAAqoKCv74QAAAAKoqCvz4RAAAAKoKCv74RAAAAyoqCPxgIAAAAZUHBHywEAAAAXyj4g4kAAABQEgr+YGMtgArr6rKsOXOUmjXLMD7+caWSyXh8xgx5JRZraPjwEGWM/BVMs1CQxUdisaGhEyeUisdPnTp2TKlIJJuVtccrxbIMo1iUddGjUcm/pyeV6ulR6v33Gxvff19WSYvHBwa8gyuorS0abW1V6pZbOjpuvFGp+fMbGubOlfNlmomEdxCAfyWbtW0pgPfty2Teflup557r63vhBaWOHy8Uuru9gyoone7uHhz0dmrgtdduvvnpp+UOilIQAJRZJCKFt1JLl9r26tVSoDc1XX65vGKM+PCQCyLHyqdKJE6cOHpUqVTqgw/efVdecRzb/vCQipK85d+BA83NBw8qtWdPW9vu3ZJmGOXMXxf83/jGjBlf/apSDQ2mqQMjAOOTTtu2BPLf+c6hQw88oNSJE8XiyZPeixVAABBsNAGUiS74ly0zTSnIYrHmZnmCHW/Br0nhKz+Xzba1TZum1NDQzJmLF8sr7vKklSZ5y7/ZswcHL7lEPld397JlkuY45cxfP/FT8AP+yfdIlim++eaOjhtu8BKBMRAAlMmSJY4jT/yySvjkyV5iGRUKjY3ypJxOX3TRpZd6iVXU2ZnNdnYqtWjRyZMLF3qJZaCr+gGUz4IFfK9wfgQAPuk2/ni8sdGt6q+sfL69fepUabtPJBobvcQqmjVrYEBqBJqbC4XmZi/RB9r4gfKT71Uy6e0AYyAA8El37pNn/2q0ROmmgXy+re3ii73EKtJNAzNnDg7OmuUlBkgmk8tZllKHD/f2SudKvc1k8nlJrzTyJ/9a5g+cjgDAp8QIt1d/deXzDQ3t7d5ODXR25nLSJBA
                            U+sa7b9+RI9Ipqrv71CnpLKW3+/YdPizplboRkz/51zJ/4GwIAHyLRmvRec1xYrFaVvE1NBSLQeq019s7OJjNKmXbzggv8TQ6va9vYEBuyOVG/uRfy/yBsyEA8El6xXv/rbJqNDicW60+eSlqfbbI3/tPjYQ9f+BsCAB8chzLSqe9nSoyjHxenihqRU8cFBQdHS0tUmNimmfvqyHpsu3sbGmRYVTlRv7kX8v8gbMhAPApN0JmzKu2WGx4uL/f26mB3t5kUmYMDIpUKh6XuRrmzp0+XYZTdnVNmiQ3ZL2dN2/69LY2d6ZGOa7cyJ/8a5k/cDYEAD4dPOg4mza5VXzVqBLX+SSTJ09+8IGXWEWSt/w7eLCpqRaBj1/6Rjx9emdnU9Potlo3XvIn/1rmD5yOAMCn7u5I5J13pCZgaGjfPi+xguLx/v4jR6TKMJeT4UPVJlMDHzig1NBQLFaOKUD1XOYAyieTsaxaNhEiGAgAymTHDqV+/GN5Os5kKrEIh1T5yyJBqdTx4++95yVWUU+PW+W/d6+7JkC56EVMAJTP3r2ZzFtveTvAGAgAysS2pVueUq++alnf+56M0x8aevPN0psG9M8lEv39hw8rJWvz7dol6dVdDGj//ubm/fuV2r69q+s3v3E/ZymfZyx69TK9iAmA0g0P27bUDD77bF/f8897icAYPux5GmT1vhzw5Mm2LXP3y/S5MmNgIhGPz5wpr8RiMpWv2/dXtm6vft25T7fxV7qqv1g0DJl4JJ2WnEc79+k2/nJV9Z+PXhVQL2Ki5zJnSlNgbNKEJveNPXvSaXni1wW/rAJ46pR3UAWxGmCwEQAAAEpCABBsNAEAABBCBAAAAIQQAQAAACFEAAAAQAgRAAAAEEIEAAAAhBABAAAAIUQAAABACBEAAAAQQgQAAACEEAEAAAAhxFoAFdbVZVlz5ig1a5ZhyGJAyWQ8PmOGvBKLNTR8eMiHCwKZZqEgq+HFYkNDsuxvPH7q1LFjSkUi2WwlFwOyLMMoFmVO72hU8u/pSaVkMaD335f1B5UaHIzHBwa8gytILwZ0yy0dHTfeqNT8+aOLASUS3kEA/hVZDCiXG11WW6+uefx4oVCJZcnPxFoAwUYAUGaRiBTeSi1daturV0uB3tR0+eXyijHiw0MuiBwrnyqROHHi6FGlUqkPPnj3XXmlussBHzjQ3HzwoKw21ta2e7ekGUY589cF/ze+MWPGV7+qVEODaerACMD46GW1v/OdQ4ceeMBdFfDkSe/FCiAACDaaAMpEF/zLlpmmFGSxWHOz
                            PMGOt+DXpPCVn8tm29qmTVNqaGjmzMWL5RXDMKvwV5O85d/s2YODspTxsmXd3cuWSZrjlDN//cRPwQ/4J9+jVGp0WW3gXAgAymTJEseRJ37DSKUmT/YSy6hQaGyUJ+V0+qKLLr3US6yizs5strNTqUWLTp5cuNBLLANd1Q+gfBYs4HuF8yMA8Em38cfjjY1uVX9l5fPt7VOnStt9ItHY6CVW0axZAwNSI9DcXCg0N3uJPtDGD5SffK+SSW8HGAMBgE+6c588+1ejJUo3DeTzbW0XX+wlVpFuGpg5c3Bw1iwvMUAymVzOspQ6fLi3VzpX6m0mk89LeqWRP/nXMn/gdAQAPiVGuL36qyufb2hob/d2aqCzM5eTJoGg0DfeffuOHJFOUd3dp05JZym93bfv8GFJr9SNmPzJv5b5A2dDAOBbNFqLzmuOE4vVsoqvoaFYDFKnvd7ewcFsVinbdkZ4iafR6X19AwNyQy438if/WuYPnA0BgE/SK977b5VVo8Hh3Gr1yUtR67NF/t5/aiTs+QNnQwDgk+NYVjrt7VSRYeTz8kRRK3rioKDo6GhpkRoT0zx7Xw1Jl21nZ0uLDKMqN/In/1rmD5wNAYBPuREyY161xWLDw/393k4N9PYmkzJjYFCkUvG4zNUwd+706TKcsqtr0iS5IevtvHnTp7e1uTM1ynHlRv7kX8v8gbMhAPDp4EHH2bTJreKrRpW4zieZPHnygw+8xCqSvOXfwYNNTbUIfPzSN+Lp0zs7m5pGt9W68ZI/+dcyf+B0BAA+dXdHIu+8IzUBQ0P79nmJFRSP9/cfOSJVhrmcDB+qNpka+MABpYaGYrFyTAGq5zIHUD6ZjGXVsokQwUAAUCY7dij14x/L03EmU4lFOKTKXxYJSqWOH3/vPS+xinp63Cr/vXvdNQHKRS9iAqB89u7NZN56y9sBxkAAUCa2Ld3ylHr1Vcv63vdknP7Q0Jtvlt40oH8ukejvP3xYKVmbb9cuSa/uYkD79zc379+v1PbtXV2/+Y37OUv5PGPRq5fpRUwAlG542LalZvDZZ/v6nn/eSwTG8GHP0yCr9+WAJ0+2bZm7X6bPlRkDE4l4fOZMeSUWk6l83b6/snV79evOfbqNv9JV/cWiYcjEI+m05DzauU+38Zerqv989KqAehETPZc5U5oCY5MmNLlv7NmTTssTvy74ZRXAU6e8gyqI1QCDjQAAAFASAoBgowkAAIAQIgAAACCECAAAAAghAgAAAEKIAAAAgBAiAAAAIIQIAAAACCHmAQCACpszx7ZlYp7ly
                            4vFY8eUuuIK2+7tVaq93XFkLYyuLts+fVnx7m7TbGhQqr/fMBIJpXbtMs3OTqW2bYtGp0xR6u23TXPSJO/gGmIegGAjAACAMtFF0cqVliWLdq1enc/LlOAzZtj20JD7WjkcOmSasorgE0/E4wsWKPXKK5HI1KmlTTvuBwFAsNEEAAA+TZniODKV9oMPZrObNyt1773Z7G9/W/6CX9O/d926bHb7dqW+//1MZssWpS6+2HFOr0kAzoUAAABKtGiRbff1KfXww5mMFPwLFliWrOVRbQsXuu/jRz/KZF5+Wakrr7QsaWIAzoUAAADGafFit8B94IFsdutWpVpb3bb8WmtpcRxZlfT++3O5bdtG3ydwNgQAAHCBdBW7VPG/9ppS0Wh1luceL/2+1q933+fUqe4ywcDpCAAA4Dx0N7Ovfz2X27HDfeKXJ+16p2sE7rknl3v99dHPAQgCAAA4j5Uri0Xp1V+rNn6/dB+BFSuKxaNHvUSEHgEAAIxBPzGvXl0oyHA+/9yBetKQEIkoJcVyLKaUZR0/Ho+PbnW6Pk6U4+ldD0sEBAEAAIzhssvcCXz8DueTFnkpwG27vz8ale3AgBTsjlMouAW7HsGvAwQ3XR8nAYH8nP49pZo1y3Fk3L7+XAi3MsSUtWVZv/xlRwcTAQGoR+69SRf8UrCbPh67DEO69408uZmdncWim1bK3W/Dhnh83jzZxmKyLRUTAQUbNQAAMAa/jxaOk8noJ30/Bb/mOMWiFHfSNOCn2Fu8uFhkeCAIAABgDH6fLSUAKEfBfyYdWJSqo0OpTMbbQWgRAABAhegn9nLz+3s7O207m/V2EFoEAAAQMo5TibAEQUMAAAAVojvtlZv8Xj8zEPb1GUYy6e0gtAgAAKBCpJitxFTBhtHQ4C8AUCqV8nYQWgQAAFAhUlBblmxjsXLUBOjfYxiplJ/ft3NnNCodARFugW8HWrVqy5Y776xEJRuAsJszx50w55FHMpmNG73EEugJfBznxAk9H8B4WuFHC/7WVhn/bxiRiJ+73po1qdSqVUq9845pTprkJZaAeQCCjRoAABiDLiAPHTLNpiYvsQSGYZpSYJtmR4cU4KbZ0qJrBtyqfF2IuVudLrm7x7s/57fgP3jQMJqb/Rf8mBgIAABgDLqwfeKJeHzBAvf/frlP8m7TgC7YI5GLLpJV+/R2tMBPpXRbv5+CX3viiUSiXJ8DwUcAAADn8corkcjUqUrt2SNFs5cYILt3u+9769ZIZMoULxGhRwAAAOehn76/+91EYskSpU6eNIxEwk2rZw
                            MDhiGrC95/fzIp7xs4HQEAAFyg48dNs6FBqW99K5n82MeUkvn4KjHVr1/6fa1bl0hcdZVSx45Jo4P3IuAhAACAcdq5061Sv/vuZHLFCqVOnKiPGgH9xH/PPYnE8uVK7doViTDcD2MhAACAEum29S99KZW65pra9RHQ72Pt2mTy2muV+t3vIpHOTu9FYAwEAADgk1SxNzYq9eUvp1IrVyq1fn0iIU0Eethduenfq6v4db66iQK4EIGfQIGJgADUu8sucycUWr7cso4dG12Pv71dKVmVb/Jk2z59ed6eHtOUqXr7+5WSOfv1zH3btrm9+OtlHD8TAQVb8AOAb23+71/Yattqo/Fx4wkuBACoNJnZULaZTG9vTQKAmLpH/Xxg4LVXbn7n6TRTGpUq8E0A9jedb6vrZeoMAEA1SACgJyiqiYfVZ9XU99/39lCi4AcAe4tXWvPffNPbBQBUmG3n8zJTYa04Txn/YhgvvujtokSBDwCKrzg/sh6+7z5vFwBQYcViNlsoeDtV5PxUpVTEstSfFD9vf+Tv/s5LRokCHwC8etd1S3+69LnnrO7cR4rP79rlJQMAyqxYzGSkwdW2i8WaNAHsVzeo+T/84fY5t976zDN793qpKNGEGQZo7I40pT++fLn936z77F3SdxYAUA5S5S+LF+XzQ0O5nJdYRc7/UIvVBy+9ZP3X3h+0Hrr7bi8ZPk2YAGCTscp45mdDQ+qTzq+HZs6aZX0s925x7s6d3ssAgHHST/zZ7KlT6bSkVGfIta7qd76tPqMWPfSQ9eXe/9M691Of+pfX71rzN39bi8aHiWnCD5tbfssvf/653I03RqfHphn9995r7oxkIt9ZsEBdE3nZ+M+JhLFJrWT4IIBwckbIE/5IYfvh1u3cp9v4K13V73xfXa6ahoaMpHpGbT5w4P937vPa+KnqBwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgrJT6f/Hmo7/TWXW4AAAAAElFTkSuQmCC'''
            icon_image = tk.PhotoImage(data=icon_data)
            self.root.iconphoto(True, icon_image)
        except (tk.TclError, Exception) as e:
            print(f"Failed to load icon: {e}")

    def on_closing(self):
        """Handle application closing"""
        self.cleanup()
        self.root.destroy()

    def cleanup(self):
        """Clean up resources before closing"""
        if getattr(self, '_cleanup_called', False):
            return
        self._cleanup_called = True
        
        print("Cleaning up resources...")
        
        # Cancel timers using proper attribute names
        for timer_attr in ['table1_scroll_timer', 'table2_scroll_timer', 'search_scroll_timer']:
            if hasattr(self, timer_attr) and getattr(self, timer_attr) is not None:
                self.root.after_cancel(getattr(self, timer_attr))
                print(f"Cancelled {timer_attr}")
        
        # Clear matplotlib resources
        plt.close('all')
        
        # Clear data structures
        self.pandas_dfs.clear()
        self.polars_dfs.clear()
        
        print("Cleanup completed")

    def _get_timer_attr_name(self, table_state: TableState) -> str:
        """Get proper timer attribute name for table state"""
        if table_state == self.table1_state:
            return 'table1_scroll_timer'
        elif table_state == self.table2_state:
            return 'table2_scroll_timer' 
        elif table_state == self.search_state:
            return 'search_scroll_timer'
        else:
            return 'table1_scroll_timer'

    def _setup_keyboard_shortcuts(self):
        """Setup keyboard shortcuts for the application"""
        # Bind Ctrl+F (Windows/Linux) and Cmd+F (Mac) for search
        self.root.bind('<Control-f>', self._handle_search_shortcut)
        self.root.bind('<Control-F>', self._handle_search_shortcut)
        
        # Mac Command+F
        self.root.bind('<Command-f>', self._handle_search_shortcut)
        self.root.bind('<Command-F>', self._handle_search_shortcut)
        
        print("Keyboard shortcuts enabled: Ctrl+F / Cmd+F for search")

    def _handle_search_shortcut(self, event):
        """Handle Ctrl+F / Cmd+F keyboard shortcut"""
        try:
            # Get the currently selected tab
            selected_tab = self.notebook.select()
            tab_text = self.notebook.tab(selected_tab, "text")
            
            # Only trigger search in Table-1 or Table-2 tabs
            if tab_text == Config.TAB_TABLE1:
                if self.table1_state.current_table_df is not None:
                    self.show_search_dialog(self.table1_state)
                    return "break"  # Prevent default behavior
                else:
                    print("Ctrl+F: No data loaded in Table-1")
                    
            elif tab_text == Config.TAB_TABLE2:
                if self.table2_state.current_table_df is not None:
                    self.show_search_dialog(self.table2_state)
                    return "break"
                else:
                    print("Ctrl+F: No data loaded in Table-2")
                    
            elif tab_text == Config.TAB_SEARCH_RESULTS:
                print("Ctrl+F: Search not available in Search Results tab")
                
            elif tab_text == Config.TAB_PLOTTER:
                print("Ctrl+F: No search available in Plotter tab")
                
        except tk.TclError as e:
            print(f"Error handling search shortcut: {e}")
        
        return "break"  # Prevent default Ctrl+F behavior

    def setup_gui(self):
        """Setup the GUI layout"""
        main_frame = self._create_main_container()
        left_frame = self._create_left_panel(main_frame)
        right_frame = self._create_right_panel(main_frame)
        
        self._setup_control_buttons(left_frame)
        self._setup_variable_tree(left_frame)
        self._setup_notebook_tabs(right_frame)

    def _create_main_container(self):
        """Create main container frame"""
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        return main_frame

    def _create_left_panel(self, parent):
        """Create left panel for variable selection"""
        left_frame = ttk.LabelFrame(parent, text=Config.LABEL_LOAD_ANALYZE, 
                                   width=Config.LEFT_PANEL_WIDTH)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 5))
        left_frame.pack_propagate(False)
        return left_frame

    def _create_right_panel(self, parent):
        """Create right panel with tabs"""
        right_frame = ttk.Frame(parent)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        return right_frame

    def _setup_control_buttons(self, parent):
        """Setup control buttons"""
        control_frame = ttk.Frame(parent)
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(control_frame, text=Config.BTN_LOAD_LOG, 
                  command=self.load_log_file).pack(fill=tk.X, pady=2)
        ttk.Button(control_frame, text=Config.BTN_PLOT_SELECTED, 
                  command=self.plot_selected).pack(fill=tk.X, pady=2)
        ttk.Button(control_frame, text=Config.BTN_CLEAR_PLOT, 
                  command=self.clear_plot).pack(fill=tk.X, pady=2)

    def _setup_variable_tree(self, parent):
        """Setup variable tree with scrollbar"""
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        tree_scroll = ttk.Scrollbar(tree_frame)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.variable_tree = ttk.Treeview(tree_frame, yscrollcommand=tree_scroll.set)
        self.variable_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tree_scroll.config(command=self.variable_tree.yview)
        
        self._configure_variable_tree()
        self._bind_tree_events()

    def _configure_variable_tree(self):
        """Configure treeview columns and headings"""
        self.variable_tree["columns"] = ("type", "count", "full_name")
        self.variable_tree.column("#0", width=200, minwidth=150)
        self.variable_tree.column("type", width=80, minwidth=50)
        self.variable_tree.column("count", width=80, minwidth=50)
        self.variable_tree.column("full_name", width=0, minwidth=0, stretch=False)
        
        self.variable_tree.heading("#0", text=Config.TREE_HEADER_VARIABLES, anchor=tk.W)
        self.variable_tree.heading("type", text=Config.TREE_HEADER_TYPE, anchor=tk.W)
        self.variable_tree.heading("count", text=Config.TREE_HEADER_COUNT, anchor=tk.W)
        
        # Configure tags for highlighting
        self.variable_tree.tag_configure("plotted_left", 
                                        background=Config.PLOTTED_LEFT_BG, 
                                        foreground=Config.PLOTTED_LEFT_FG)
        self.variable_tree.tag_configure("plotted_right", 
                                        background=Config.PLOTTED_RIGHT_BG, 
                                        foreground=Config.PLOTTED_RIGHT_FG)

    def _bind_tree_events(self):
        """Bind tree event handlers"""
        self.variable_tree.bind("<Double-1>", self.on_tree_double_click)
        self.variable_tree.bind("<Double-Button-3>", self.on_tree_right_double_click)

    def _setup_notebook_tabs(self, parent):
        """Setup notebook tabs"""
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Plot tab
        self.plot_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.plot_frame, text=Config.TAB_PLOTTER)
        
        # Table-1 tab
        self.table1_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.table1_frame, text=Config.TAB_TABLE1)
        
        # Table-2 tab
        self.table2_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.table2_frame, text=Config.TAB_TABLE2)
        
        # Search Results tab
        self.search_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.search_frame, text=Config.TAB_SEARCH_RESULTS)
        
        # Setup tabs
        self.setup_plot_tab()
        self.setup_table_tab(self.table1_frame, self.table1_state, Config.TAB_TABLE1)
        self.setup_table_tab(self.table2_frame, self.table2_state, Config.TAB_TABLE2)
        self.setup_search_results_tab()
        
        # Bind tab change event
        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)

    def on_tab_changed(self, event):
        """Handle tab change to track active table state"""
        try:
            selected_tab = self.notebook.select()
            tab_text = self.notebook.tab(selected_tab, "text")
            
            if tab_text == Config.TAB_TABLE1:
                self.active_table_state = self.table1_state
            elif tab_text == Config.TAB_TABLE2:
                self.active_table_state = self.table2_state
            elif tab_text == Config.TAB_SEARCH_RESULTS:
                self.active_table_state = self.search_state
        except tk.TclError:
            pass

    def setup_plot_tab(self):
        """Setup the plotting tab"""
        self._create_matplotlib_figure()
        self._setup_navigation_toolbar()
        self._setup_plot_options()

    def _create_matplotlib_figure(self):
        """Create matplotlib figure with dual y-axes"""
        self.fig, self.ax = plt.subplots(figsize=Config.PLOT_FIGURE_SIZE)
        self.ax2 = None
        
        self.canvas = FigureCanvasTkAgg(self.fig, self.plot_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    def _setup_navigation_toolbar(self):
        """Setup matplotlib navigation toolbar"""
        self.toolbar = NavigationToolbar2Tk(self.canvas, self.plot_frame)
        self.toolbar.update()

    def _setup_plot_options(self):
        """Setup plot options frame"""
        options_frame = ttk.Frame(self.plot_frame)
        options_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(options_frame, text=Config.LABEL_Y_AXIS_SCALE).pack(side=tk.LEFT)
        self.y_scale_var = tk.StringVar(value="linear")
        ttk.Radiobutton(options_frame, text=Config.LABEL_LINEAR, variable=self.y_scale_var, 
                       value="linear").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(options_frame, text=Config.LABEL_LOG, variable=self.y_scale_var, 
                       value="log").pack(side=tk.LEFT, padx=5)
        
        ttk.Label(options_frame, text=Config.LABEL_LEFT_AXIS, 
                 foreground=Config.LEFT_AXIS_COLOR).pack(side=tk.LEFT, padx=10)
        ttk.Label(options_frame, text=Config.LABEL_RIGHT_AXIS, 
                 foreground=Config.RIGHT_AXIS_COLOR).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(options_frame, text=Config.BTN_REFRESH_PLOT, 
                  command=self.refresh_plot).pack(side=tk.RIGHT, padx=5)

    def setup_table_tab(self, parent_frame, table_state, tab_name):
        """Setup a table viewing tab with search functionality"""
        tab_refs = {}
        self.table_tabs[tab_name] = tab_refs
        
        self._setup_table_controls(parent_frame, table_state, tab_name, tab_refs)
        self._setup_table_container(parent_frame, table_state, tab_name, tab_refs)

    def _setup_table_controls(self, parent_frame, table_state, tab_name, tab_refs):
        """Setup table controls frame with search button"""
        table_controls = ttk.Frame(parent_frame)
        table_controls.pack(fill=tk.X, padx=5, pady=5)
        
        info_label = ttk.Label(table_controls, text=Config.LABEL_NO_DATA)
        info_label.pack(side=tk.LEFT)
        tab_refs['info_label'] = info_label
        
        ttk.Button(table_controls, text=Config.BTN_SHOW_HIDE_COLS, 
                command=lambda: self.show_column_management_dialog(table_state, tab_refs)).pack(side=tk.LEFT, padx=10)
        
        hidden_label = ttk.Label(table_controls, text="", foreground="gray")
        hidden_label.pack(side=tk.LEFT, padx=5)
        tab_refs['hidden_label'] = hidden_label

        button_frame = ttk.Frame(table_controls)
        button_frame.pack(side=tk.RIGHT)
        
        # Search button
        search_btn = ttk.Button(button_frame, text=Config.BTN_SEARCH, 
                               command=lambda: self.show_search_dialog(table_state))
        search_btn.pack(side=tk.RIGHT, padx=5)
        tab_refs['search_btn'] = search_btn
        
        ttk.Button(button_frame, text=Config.BTN_EXPORT_CSV, 
                command=lambda: self.export_current_table_to_csv(table_state)).pack(side=tk.RIGHT, padx=5)

        ttk.Button(button_frame, text=Config.BTN_LOAD_ALL_ROWS, 
                command=lambda: self.load_all_rows(table_state, tab_refs)).pack(side=tk.RIGHT, padx=5)

    def _setup_table_container(self, parent_frame, table_state, tab_name, tab_refs):
        """Setup table container with proper event binding"""
        table_container = ttk.Frame(parent_frame)
        table_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        table_tree = ttk.Treeview(table_container)
        tab_refs['table_tree'] = table_tree
        
        # Use lambda functions for event binding
        table_tree.bind("<Double-1>", lambda e: self.on_table_cell_double_click(e, table_state))
        table_tree.bind("<Button-3>", lambda e: self.handle_right_click(e, table_state))
        
        v_scrollbar = ttk.Scrollbar(table_container, orient=tk.VERTICAL, command=table_tree.yview)
        h_scrollbar = ttk.Scrollbar(table_container, orient=tk.HORIZONTAL, command=table_tree.xview)
        
        table_tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        table_tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        
        table_container.grid_rowconfigure(0, weight=1)
        table_container.grid_columnconfigure(0, weight=1)

    def setup_search_results_tab(self):
        """Setup search results tab"""
        search_info_frame = ttk.Frame(self.search_frame)
        search_info_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.search_details_label = ttk.Label(search_info_frame, 
                                            text=Config.LABEL_SEARCH_PLACEHOLDER, 
                                            font=('TkDefaultFont', 9, 'italic'),
                                            foreground="gray")
        self.search_details_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        search_refs = {}
        self.table_tabs[Config.TAB_SEARCH_RESULTS] = search_refs
        
        self._setup_table_controls_for_search(self.search_frame, search_refs)
        self._setup_table_container(self.search_frame, self.search_state, Config.TAB_SEARCH_RESULTS, search_refs)

    def _setup_table_controls_for_search(self, parent_frame, tab_refs):
        """Setup table controls for search results (no search button)"""
        table_controls = ttk.Frame(parent_frame)
        table_controls.pack(fill=tk.X, padx=5, pady=5)
        
        info_label = ttk.Label(table_controls, text=Config.LABEL_NO_SEARCH)
        info_label.pack(side=tk.LEFT)
        tab_refs['info_label'] = info_label
        
        ttk.Button(table_controls, text=Config.BTN_SHOW_HIDE_COLS, 
                command=lambda: self.show_column_management_dialog(self.search_state, tab_refs)).pack(side=tk.LEFT, padx=10)
        
        hidden_label = ttk.Label(table_controls, text="", foreground="gray")
        hidden_label.pack(side=tk.LEFT, padx=5)
        tab_refs['hidden_label'] = hidden_label

        button_frame = ttk.Frame(table_controls)
        button_frame.pack(side=tk.RIGHT)
        
        ttk.Button(button_frame, text=Config.BTN_EXPORT_CSV, 
                command=lambda: self.export_current_table_to_csv(self.search_state)).pack(side=tk.RIGHT, padx=5)

        ttk.Button(button_frame, text=Config.BTN_LOAD_ALL_ROWS, 
                command=lambda: self.load_all_rows(self.search_state, tab_refs)).pack(side=tk.RIGHT, padx=5)

    def show_search_dialog(self, table_state: TableState):
        """Show search dialog for the specified table state"""
        if table_state.current_table_df is None:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_NO_DATA_TABLE)
            return
        
        dialog = self._create_search_dialog(table_state)
        self._setup_search_dialog_content(dialog, table_state)

    def _create_search_dialog(self, table_state: TableState):
        """Create search dialog window"""
        dialog = tk.Toplevel(self.root)
        dialog.title(Config.DIALOG_SEARCH_TITLE.format(table_state.current_table_name))
        dialog.geometry(Config.SEARCH_DIALOG_SIZE)
        dialog.transient(self.root)
        dialog.grab_set()
        
        dialog.geometry(f"+{self.root.winfo_rootx() + 100}+{self.root.winfo_rooty() + 100}")
        return dialog

    def _setup_search_dialog_content(self, dialog, table_state: TableState):
        """Setup search dialog content with proper cleanup"""
        main_frame = ttk.Frame(dialog)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Title
        title_label = ttk.Label(main_frame, 
                               text=Config.SEARCH_LABEL_TITLE.format(table_state.current_table_name),
                               font=('TkDefaultFont', 12, 'bold'))
        title_label.pack(anchor='w', pady=(0, 10))
        
        # Search options
        search_frame = ttk.LabelFrame(main_frame, text=Config.SEARCH_TITLE_OPTIONS)
        search_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(search_frame, text=Config.SEARCH_LABEL_TERM).pack(anchor='w', padx=10, pady=(10, 5))
        search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=search_var, font=('TkDefaultFont', 11))
        search_entry.pack(fill=tk.X, padx=10, pady=(0, 10))
        search_entry.focus_set()
        
        options_frame = ttk.Frame(search_frame)
        options_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        fuzzy_var = tk.BooleanVar(value=True)
        case_var = tk.BooleanVar(value=False)
        
        ttk.Checkbutton(options_frame, text=Config.SEARCH_LABEL_FUZZY, 
                       variable=fuzzy_var).pack(anchor='w')
        ttk.Checkbutton(options_frame, text=Config.SEARCH_LABEL_CASE, 
                       variable=case_var).pack(anchor='w')
        
        # Columns selection
        columns_frame = ttk.LabelFrame(main_frame, text=Config.SEARCH_LABEL_COLUMNS)
        columns_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Proper canvas setup with cleanup
        canvas = tk.Canvas(columns_frame, height=200)
        scrollbar = ttk.Scrollbar(columns_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        scrollbar.pack(side="right", fill="y", pady=10)
        
        # Column selection
        column_vars = {}
        searchable_columns = [col for col in table_state.current_table_df.columns 
                            if col.lower() != 'timestamp']
        
        # Select all/deselect all buttons
        select_frame = ttk.Frame(scrollable_frame)
        select_frame.pack(fill=tk.X, pady=(0, 10))
        
        def select_all():
            for var in column_vars.values():
                var.set(True)
        
        def deselect_all():
            for var in column_vars.values():
                var.set(False)
        
        ttk.Button(select_frame, text=Config.BTN_SELECT_ALL, command=select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text=Config.BTN_DESELECT_ALL, command=deselect_all).pack(side=tk.LEFT, padx=5)
        
        # Create checkboxes
        for col in searchable_columns:
            var = tk.BooleanVar(value=True)
            column_vars[col] = var
            ttk.Checkbutton(scrollable_frame, text=col, variable=var).pack(anchor='w', padx=10, pady=1)
        
        # Mouse wheel binding with cross-platform support
        def _on_mousewheel(event):
            try:
                if hasattr(event, 'delta') and event.delta:  # Windows
                    canvas.yview_scroll(int(-1*(event.delta/120)), "units")
                elif hasattr(event, 'num'):  # Linux
                    if event.num == 4:
                        canvas.yview_scroll(-1, "units")
                    elif event.num == 5:
                        canvas.yview_scroll(1, "units")
            except:
                pass
        
        def bind_mousewheel(event):
            canvas.bind("<MouseWheel>", _on_mousewheel)
            canvas.bind("<Button-4>", _on_mousewheel)  # Linux
            canvas.bind("<Button-5>", _on_mousewheel)  # Linux
        
        def unbind_mousewheel(event):
            canvas.unbind("<MouseWheel>")
            canvas.unbind("<Button-4>")
            canvas.unbind("<Button-5>")
        
        canvas.bind('<Enter>', bind_mousewheel)
        canvas.bind('<Leave>', unbind_mousewheel)
        
        # Buttons
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=(10, 0))
        
        def cleanup_and_close():
            """Proper dialog cleanup"""
            try:
                canvas.unbind('<Enter>')
                canvas.unbind('<Leave>')
                canvas.unbind("<MouseWheel>")
                canvas.unbind("<Button-4>")
                canvas.unbind("<Button-5>")
            except:
                pass
            dialog.destroy()
        
        ttk.Button(buttons_frame, text=Config.BTN_CANCEL, command=cleanup_and_close).pack(side=tk.RIGHT, padx=(5, 0))
        
        def perform_search():
            search_term = search_var.get().strip()
            if not search_term:
                messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_ENTER_SEARCH_TERM)
                return
            
            selected_columns = [col for col, var in column_vars.items() if var.get()]
            if not selected_columns:
                messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_SELECT_COLUMN)
                return
            
            cleanup_and_close()
            self._perform_search(table_state, search_term, selected_columns, 
                               fuzzy_var.get(), case_var.get())
        
        ttk.Button(buttons_frame, text=Config.BTN_SEARCH, command=perform_search).pack(side=tk.RIGHT)
        
        search_entry.bind('<Return>', lambda e: perform_search())

    def _perform_search(self, table_state: TableState, search_term: str, 
                       columns: List[str], fuzzy: bool, case_sensitive: bool):
        """Perform the actual search operation"""
        try:
            df = table_state.current_table_df
            total_rows = len(df)
            
            progress_dialog = None
            if total_rows > Config.SEARCH_PROGRESS_THRESHOLD:
                progress_dialog = self.show_progress_dialog(Config.PROGRESS_SEARCHING.format(total_rows), total_rows)
            
            matching_indices = self._search_dataframe(df, search_term, columns, fuzzy, case_sensitive, progress_dialog)
            
            if progress_dialog:
                progress_dialog.close()
            
            if matching_indices:
                result_df = df.iloc[matching_indices].copy()
                search_result = SearchResult(
                    table_state.current_table_name,
                    search_term,
                    columns,
                    result_df,
                    fuzzy,
                    case_sensitive
                )
                
                self._display_search_results(search_result)
                messagebox.showinfo(Config.DIALOG_SEARCH_COMPLETE, 
                                  Config.MSG_FOUND_MATCHES.format(len(matching_indices), total_rows))
            else:
                messagebox.showinfo(Config.DIALOG_SEARCH_COMPLETE, 
                                  Config.MSG_NO_MATCHES.format(search_term))
                
        except Exception as e:
            if progress_dialog:
                progress_dialog.close()
            messagebox.showerror(Config.DIALOG_SEARCH_ERROR, f"Error during search:\n{str(e)}")
            print(f"Search error: {e}")

    def _search_dataframe(self, df: pd.DataFrame, search_term: str, columns: List[str], 
                        fuzzy: bool, case_sensitive: bool, progress_dialog=None) -> List[int]:
        """Search dataframe with properly integrated fuzzy search - FIXED array handling"""
        matching_indices = []
        total_rows = len(df)
        
        # Check if fuzzy search is available
        if fuzzy and not hasattr(difflib, 'SequenceMatcher'):
            print("Warning: difflib.SequenceMatcher not available, falling back to exact search")
            fuzzy = False
        
        if not case_sensitive:
            search_term_compare = search_term.lower()
        else:
            search_term_compare = search_term
        
        for idx in range(total_rows):
            # Progress update with proper interval
            if progress_dialog and idx % 100 == 0:
                try:
                    progress_dialog.update_progress(idx, Config.PROGRESS_SEARCH_ROW.format(idx+1, total_rows))
                    self.root.update_idletasks()
                except:
                    pass
            
            for col in columns:
                if col not in df.columns:
                    continue
                
                cell_value = df.iloc[idx][col]
                
                # FIXED: Handle arrays/lists/Series FIRST before checking pd.isna()
                try:
                    # Check if it's an array-like object first
                    if isinstance(cell_value, (list, tuple, np.ndarray, pd.Series)):
                        cell_str = self._safe_cell_to_string(cell_value)
                    # Check for scalar NaN/None
                    elif cell_value is None or (isinstance(cell_value, float) and pd.isna(cell_value)):
                        cell_str = ""
                    else:
                        cell_str = self._safe_cell_to_string(cell_value)
                except Exception as e:
                    # Fallback: just convert to string
                    print(f"Warning: Error processing cell [{idx}][{col}]: {e}")
                    try:
                        cell_str = self._safe_cell_to_string(cell_value)
                    except:
                        cell_str = ""
                
                if not case_sensitive:
                    cell_str_compare = cell_str.lower()
                else:
                    cell_str_compare = cell_str
                
                match_found = False
                
                if fuzzy:
                    # Now properly calls the fuzzy search with config parameters
                    match_found = self._fuzzy_search_in_text(search_term_compare, cell_str_compare)
                else:
                    # Exact substring search
                    if search_term_compare in cell_str_compare:
                        match_found = True
                
                if match_found:
                    matching_indices.append(idx)
                    break
        
        return matching_indices

    def _safe_cell_to_string(self, cell_value) -> str:
        """Safely convert any cell value to string for searching"""
        try:
            # Handle None
            if cell_value is None:
                return ""
            
            # Handle array-like objects
            if isinstance(cell_value, (list, tuple, np.ndarray, pd.Series)):
                return str(cell_value)
            
            # Handle scalar NaN (float type)
            if isinstance(cell_value, float):
                if pd.isna(cell_value):
                    return ""
                return str(cell_value)
            
            # Handle everything else
            return str(cell_value)
            
        except Exception as e:
            print(f"Warning: Could not convert cell value to string: {e}")
            return ""

    def _fuzzy_search_in_text(self, search_term: str, text: str) -> bool:
        """Properly integrated fuzzy search with all config parameters"""
        if not search_term or not text:
            return False
        
        # First try exact substring match (fastest and most accurate)
        if search_term in text:
            return True
        
        # Use config parameter for minimum length
        if len(search_term) < Config.FUZZY_SEARCH_MIN_LENGTH:
            return False
        
        # Try different fuzzy approaches
        sliding_match = self._sliding_window_fuzzy_match(search_term, text)
        if sliding_match:
            return True
        
        word_match = self._word_boundary_fuzzy_match(search_term, text)
        if word_match:
            return True
        
        return False

    def _sliding_window_fuzzy_match(self, search_term: str, text: str) -> bool:
        """Sliding window with config parameters and validation"""
        if len(search_term) > len(text):
            return False
        
        best_ratio = 0.0
        best_match = ""
        search_len = len(search_term)
        
        # Try window sizes close to search term length
        for window_size in [search_len, search_len + 1]:
            if window_size > len(text):
                continue
                
            for i in range(len(text) - window_size + 1):
                substring = text[i:i + window_size]
                try:
                    ratio = difflib.SequenceMatcher(None, search_term, substring).ratio()
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_match = substring
                    
                    # Use config threshold
                    if ratio >= Config.FUZZY_SEARCH_SLIDING_THRESHOLD:
                        # Now actually calls validation
                        if self._validate_fuzzy_match(search_term, substring, ratio):
                            print(f"  Fuzzy sliding match: '{search_term}' -> '{best_match}' (score: {best_ratio:.2f})")
                            return True
                except:
                    continue
        
        return False

    def _word_boundary_fuzzy_match(self, search_term: str, text: str) -> bool:
        """Word boundary matching with config parameters and validation"""
        words = re.findall(r'\b\w+\b', text)
        
        for word in words:
            # Use config parameter for length tolerance
            if abs(len(word) - len(search_term)) <= Config.FUZZY_SEARCH_LENGTH_TOLERANCE:
                if len(word) >= Config.FUZZY_SEARCH_MIN_LENGTH:
                    try:
                        ratio = difflib.SequenceMatcher(None, search_term, word).ratio()
                        
                        # Use config threshold
                        if ratio >= Config.FUZZY_SEARCH_WORD_THRESHOLD:
                            # Now actually calls validation
                            if self._validate_fuzzy_match(search_term, word, ratio):
                                print(f"  Fuzzy word match: '{search_term}' -> '{word}' (score: {ratio:.2f})")
                                return True
                    except:
                        continue
        
        return False

    def _validate_fuzzy_match(self, search_term: str, matched_text: str, ratio: float) -> bool:
        """Now actually used - validates matches to prevent false positives"""
        # Use config parameters for validation
        
        # Check if ratio meets the main threshold
        if ratio < Config.FUZZY_SEARCH_THRESHOLD:
            return False
        
        # Require good character coverage
        search_chars = set(search_term.lower())
        match_chars = set(matched_text.lower())
        common_chars = search_chars.intersection(match_chars)
        
        if len(search_chars) == 0:
            return False
        
        char_coverage = len(common_chars) / len(search_chars)
        
        # Use config parameter for character coverage
        if char_coverage < Config.FUZZY_SEARCH_CHAR_COVERAGE:
            print(f"    Rejected: '{search_term}' vs '{matched_text}' - char coverage too low ({char_coverage:.2f})")
            return False
        
        return True

    def _display_search_results(self, search_result: SearchResult):
        """Display search results in the search results tab"""
        self.current_search_result = search_result
        self.search_state.current_table_df = search_result.result_df
        self.search_state.current_table_name = f"Search Results from {search_result.source_df_name}"
        self.search_state.reset_for_new_dataframe()
        
        # Build search info using Config
        fuzzy_text = Config.SEARCH_FUZZY_ENABLED if search_result.fuzzy_enabled else Config.SEARCH_EXACT_MATCH
        case_text = Config.SEARCH_CASE_SENSITIVE if search_result.case_sensitive else Config.SEARCH_CASE_INSENSITIVE
        
        search_info = Config.SEARCH_INFO_TEMPLATE.format(
            search_result.search_term,
            search_result.source_df_name,
            ', '.join(search_result.searched_columns),
            len(search_result.searched_columns),
            len(search_result.result_df.columns),
            fuzzy_text,
            case_text,
            search_result.match_count
        )
        
        self.search_details_label.config(text=search_info)
        
        self.notebook.select(self.search_frame)
        
        search_refs = self.table_tabs[Config.TAB_SEARCH_RESULTS]
        self.refresh_table_display(self.search_state, search_refs)

    def show_column_management_dialog(self, table_state: TableState, tab_refs: dict):
        """Show column visibility management dialog for specific table state"""
        if table_state.current_table_df is None:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_NO_DATA_TABLE)
            return
        
        # Close existing dialog if open
        if self.current_column_dialog:
            try:
                self.current_column_dialog.destroy()
            except:
                pass
        
        dialog = self._create_column_dialog(table_state)
        self.current_column_dialog = dialog
        self._setup_column_dialog_content(dialog, table_state, tab_refs)

    def _create_column_dialog(self, table_state: TableState):
        """Create column management dialog window"""
        dialog = tk.Toplevel(self.root)
        dialog.title(Config.DIALOG_COLUMN_VISIBILITY.format(table_state.current_table_name))
        dialog.geometry(Config.COLUMN_DIALOG_SIZE)
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Cleanup on dialog close
        def on_dialog_close():
            self.current_column_dialog = None
            self.column_vars.clear()
            dialog.destroy()
        
        dialog.protocol("WM_DELETE_WINDOW", on_dialog_close)
        return dialog

    def _setup_column_dialog_content(self, dialog, table_state: TableState, tab_refs: dict):
        """Setup column dialog content"""
        self._create_dialog_instructions(dialog)
        self._create_dialog_action_buttons(dialog, table_state, tab_refs)
        self._create_dialog_checklist(dialog, table_state)
        self._create_dialog_bottom_buttons(dialog, table_state)
        self._finalize_dialog_setup(dialog)

    def _create_dialog_instructions(self, dialog):
        """Create dialog instructions"""
        instruction_frame = ttk.Frame(dialog)
        instruction_frame.pack(fill=tk.X, padx=20, pady=10)
        
        ttk.Label(instruction_frame, text=Config.COL_DIALOG_TITLE, 
                font=('TkDefaultFont', 12, 'bold')).pack(anchor='w')
        ttk.Label(instruction_frame, text=Config.COL_DIALOG_SUBTITLE, 
                foreground="gray").pack(anchor='w', pady=(5, 0))

    def _create_dialog_action_buttons(self, dialog, table_state: TableState, tab_refs: dict):
        """Create top action buttons"""
        top_button_frame = ttk.Frame(dialog)
        top_button_frame.pack(fill=tk.X, padx=20, pady=10)
        
        ttk.Button(top_button_frame, text=Config.BTN_HIDE_ALL, 
                command=lambda: self.hide_all_with_close(dialog, table_state, tab_refs)).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_button_frame, text=Config.BTN_UNHIDE_ALL, 
                command=lambda: self.unhide_all_with_close(dialog, table_state, tab_refs)).pack(side=tk.LEFT, padx=5)
        ttk.Button(top_button_frame, text=Config.BTN_CANCEL, 
                command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        ttk.Button(top_button_frame, text=Config.BTN_APPLY_CLOSE, 
                command=lambda: self.apply_column_visibility(dialog, table_state, tab_refs)).pack(side=tk.RIGHT, padx=5)

    def _create_dialog_checklist(self, dialog, table_state: TableState):
        """Create scrollable checklist area"""
        list_container = ttk.Frame(dialog)
        list_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        canvas = tk.Canvas(list_container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(list_container, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        self._create_column_checkboxes(scrollable_frame, table_state)
        self._setup_dialog_mouse_wheel(canvas)

    def _create_column_checkboxes(self, parent, table_state: TableState):
        """Create column checkboxes"""
        self.column_vars.clear()
        all_columns = [col for col in table_state.current_table_df.columns if col.lower() != 'timestamp']
        
        for col_name in all_columns:
            is_visible = col_name not in table_state.hidden_columns
            var = tk.BooleanVar(value=is_visible)
            self.column_vars[col_name] = var
            
            checkbox = ttk.Checkbutton(parent, text=col_name, variable=var)
            checkbox.pack(anchor='w', pady=2, padx=10, fill='x')

    def _setup_dialog_mouse_wheel(self, canvas):
        """Setup mouse wheel scrolling with cross-platform support"""
        def _on_mousewheel(event):
            try:
                if hasattr(event, 'delta') and event.delta:  # Windows
                    canvas.yview_scroll(int(-1*(event.delta/120)), "units")
                elif hasattr(event, 'num'):  # Linux
                    if event.num == 4:
                        canvas.yview_scroll(-1, "units")
                    elif event.num == 5:
                        canvas.yview_scroll(1, "units")
            except:
                pass
        
        def bind_mousewheel(event):
            canvas.bind("<MouseWheel>", _on_mousewheel)
            canvas.bind("<Button-4>", _on_mousewheel)
            canvas.bind("<Button-5>", _on_mousewheel)
        
        def unbind_mousewheel(event):
            canvas.unbind("<MouseWheel>")
            canvas.unbind("<Button-4>")
            canvas.unbind("<Button-5>")
        
        canvas.bind('<Enter>', bind_mousewheel)
        canvas.bind('<Leave>', unbind_mousewheel)

    def _create_dialog_bottom_buttons(self, dialog, table_state: TableState):
        """Create bottom selection buttons and status"""
        bottom_button_frame = ttk.Frame(dialog)
        bottom_button_frame.pack(fill=tk.X, padx=20, pady=10)
        
        ttk.Button(bottom_button_frame, text=Config.BTN_SELECT_ALL, 
                command=lambda: self.select_all_columns(True)).pack(side=tk.LEFT, padx=5)
        ttk.Button(bottom_button_frame, text=Config.BTN_DESELECT_ALL, 
                command=lambda: self.select_all_columns(False)).pack(side=tk.LEFT, padx=5)
        
        all_columns = [col for col in table_state.current_table_df.columns if col.lower() != 'timestamp']
        visible_count = len([col for col in all_columns if col not in table_state.hidden_columns])
        hidden_count = len(all_columns) - visible_count
        
        status_label = ttk.Label(bottom_button_frame, 
                            text=Config.COL_DIALOG_STATUS.format(visible_count, hidden_count), 
                            foreground="gray")
        status_label.pack(side=tk.RIGHT, padx=5)

    def _finalize_dialog_setup(self, dialog):
        """Finalize dialog setup"""
        dialog.update_idletasks()
        for widget in dialog.winfo_children():
            if hasattr(widget, 'winfo_children'):
                for child in widget.winfo_children():
                    if isinstance(child, tk.Canvas):
                        child.configure(scrollregion=child.bbox("all"))
        
        dialog.focus_set()

    def hide_all_with_close(self, dialog, table_state: TableState, tab_refs: dict):
        """Hide all columns and close dialog"""
        if not self.column_vars:
            dialog.destroy()
            return
        
        for var in self.column_vars.values():
            var.set(False)
        
        table_state.hidden_columns.clear()
        
        for col_name in self.column_vars.keys():
            table_state.hidden_columns.add(col_name)
        
        self.refresh_table_display(table_state, tab_refs)
        dialog.destroy()
        
        print(f"Auto-applied: Hidden all columns except timestamp ({len(table_state.hidden_columns)} columns)")

    def unhide_all_with_close(self, dialog, table_state: TableState, tab_refs: dict):
        """Unhide all columns and close dialog"""
        if not self.column_vars:
            dialog.destroy()
            return
        
        for var in self.column_vars.values():
            var.set(True)
        
        old_count = len(table_state.hidden_columns)
        table_state.hidden_columns.clear()
        
        self.refresh_table_display(table_state, tab_refs)
        dialog.destroy()
        
        print(f"Auto-applied: Unhidden all columns ({old_count} columns restored)")

    def select_all_columns(self, select_state):
        """Select or deselect all columns in the checklist"""
        for var in self.column_vars.values():
            var.set(select_state)

    def apply_column_visibility(self, dialog, table_state: TableState, tab_refs: dict):
        """Apply column visibility changes based on checkbox states"""
        if not self.column_vars:
            dialog.destroy()
            return
        
        old_hidden_count = len(table_state.hidden_columns)
        table_state.hidden_columns.clear()
        
        for col_name, var in self.column_vars.items():
            if not var.get():
                table_state.hidden_columns.add(col_name)
               
        new_hidden_count = len(table_state.hidden_columns)
        all_columns_count = len(self.column_vars)
        visible_count = all_columns_count - new_hidden_count
        
        print(f"Column visibility updated: {visible_count} visible, {new_hidden_count} hidden")
        
        dialog.destroy()
        self.refresh_table_display(table_state, tab_refs)

    def load_all_rows(self, table_state: TableState, tab_refs: dict):
        """Load all rows at once for specific table state"""
        if table_state.current_table_df is None:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_NO_DATA_TABLE)
            return
        
        # FIXED: Check if already loaded
        if table_state.all_rows_loaded:
            messagebox.showinfo(
                Config.DIALOG_SUCCESS, 
                "All rows are already loaded!"
            )
            return
        
        try:
            df = table_state.current_table_df
            total_rows = len(df)
            
            if total_rows > Config.LARGE_DATASET_WARNING:
                response = messagebox.askyesno(
                    Config.DIALOG_LARGE_DATASET, 
                    Config.MSG_LOAD_ALL_WARNING.format(total_rows)
                )
                if not response:
                    return
            
            # FIXED: Always show progress dialog
            progress_dialog = self.show_progress_dialog(
                Config.PROGRESS_LOADING_ROWS, 
                total_rows
            )
            
            self._load_all_rows_implementation(table_state, tab_refs, df, total_rows, progress_dialog)
            
        except Exception as e:
            print(f"Error loading all rows: {e}")
            messagebox.showerror(Config.DIALOG_ERROR, Config.MSG_LOAD_ALL_ROWS_ERROR.format(str(e)))

    def _load_all_rows_implementation(self, table_state: TableState, tab_refs: dict, 
                                    df, total_rows, progress_dialog):
        """Implementation of load all rows - RESPONSIVE PROGRESS"""
        table_tree = tab_refs.get('table_tree')
        if not table_tree:
            if progress_dialog:
                progress_dialog.close()
            return
        
        # Clear existing rows
        for item in table_tree.get_children():
            table_tree.delete(item)
        
        visible_columns = [col for col in df.columns if col not in table_state.hidden_columns]
        total_columns = len(visible_columns)
        
        print(f"Loading all {total_rows:,} rows with {total_columns} visible columns...")
        
        # RESPONSIVE: Update progress EVERY chunk (like your original code)
        for start_row in range(0, total_rows, Config.LOAD_ALL_CHUNK_SIZE):
            end_row = min(start_row + Config.LOAD_ALL_CHUNK_SIZE, total_rows)
            
            # Update progress BEFORE inserting (smooth, responsive updates)
            if progress_dialog:
                try:
                    percentage = int((end_row / total_rows) * 100)
                    progress_dialog.update_progress(
                        end_row, 
                        f"Loading rows {start_row+1:,} to {end_row:,}  ({percentage}%)"
                    )
                    self.root.update_idletasks()  # Keep UI responsive
                except:
                    pass
            
            # Insert the batch
            self._insert_batch_rows(table_tree, df, visible_columns, total_columns, start_row, end_row)
        
        # Close progress dialog
        if progress_dialog:
            progress_dialog.close()
        
        # Properly disable lazy loading
        table_state.all_rows_loaded = True
        table_state.table_current_batch = table_state.table_total_batches
        
        # Cancel timer
        timer_attr = self._get_timer_attr_name(table_state)
        if hasattr(self, timer_attr) and getattr(self, timer_attr) is not None:
            self.root.after_cancel(getattr(self, timer_attr))
            setattr(self, timer_attr, None)
            print("Cancelled scroll monitoring timer")
        
        # FIXED: Disable button after loading
        load_all_btn = tab_refs.get('load_all_btn')
        if load_all_btn:
            load_all_btn.config(state='disabled')
        
        self.update_table_info_label_all_loaded(table_state, tab_refs)
        
        print(f"Successfully loaded all {total_rows:,} rows - lazy loading disabled")
        # messagebox.showinfo(Config.DIALOG_COMPLETE, Config.MSG_ALL_ROWS_LOADED.format(total_rows))

    def refresh_table_display(self, table_state: TableState, tab_refs: dict):
        """Refresh table display for specific table state"""
        if table_state.current_table_df is None:
            return
        
        try:
            df = table_state.current_table_df
            table_tree = tab_refs.get('table_tree')
            
            if not table_tree:
                print(f"Error: table_tree not found for {table_state.current_table_name}")
                return
            
            # FIXED: Remember if all rows were loaded
            was_fully_loaded = table_state.all_rows_loaded
            
            self._clear_and_configure_table(table_tree, df, table_state)
            self._configure_table_columns(table_tree, df, table_state)
            
            # FIXED: Restore all rows if they were previously loaded
            if was_fully_loaded:
                total_rows = len(df)
                visible_columns = [col for col in df.columns if col not in table_state.hidden_columns]
                total_columns = len(visible_columns)
                
                print(f"Restoring all {total_rows:,} previously loaded rows...")
                self._insert_batch_rows(table_tree, df, visible_columns, total_columns, 0, total_rows)
                
                # Maintain the fully-loaded state
                table_state.all_rows_loaded = True
                table_state.table_batch_size = Config.TABLE_BATCH_SIZE
                table_state.table_current_batch = table_state.table_total_batches
                table_state.table_total_batches = (total_rows + table_state.table_batch_size - 1) // table_state.table_batch_size
            else:
                # Normal lazy loading initialization
                self._initialize_lazy_loading(table_state, df)
            
            self._setup_scroll_events(table_tree, table_state, tab_refs)
            
        except Exception as e:
            print(f"Error in refresh_table_display: {e}")
            messagebox.showerror(Config.DIALOG_TABLE_ERROR, Config.MSG_TABLE_ERROR.format(str(e)))

    def _clear_and_configure_table(self, table_tree, df, table_state: TableState):
        """Clear and configure table for new data"""
        for item in table_tree.get_children():
            table_tree.delete(item)
        
        all_columns = list(df.columns)
        visible_columns = [col for col in all_columns if col not in table_state.hidden_columns]
        
        table_tree["columns"] = visible_columns
        table_tree["show"] = "headings"
        
        total_columns = len(visible_columns)
        total_rows = len(df)
        
        print(f"Displaying {table_state.current_table_name} with {total_columns} visible columns ({len(table_state.hidden_columns)} hidden) and {total_rows} rows")

    def _configure_table_columns(self, table_tree, df, table_state: TableState):
        """Configure table column headings and widths"""
        visible_columns = [col for col in df.columns if col not in table_state.hidden_columns]
        total_columns = len(visible_columns)
        
        for col in visible_columns:
            try:
                table_tree.heading(col, text=str(col))
                
                col_width = self.calculate_column_width(df[col], col, total_columns)
                
                if col.lower() == 'timestamp':
                    timestamp_width = Config.TIMESTAMP_WIDTH_EXTENDED if total_columns > Config.MANY_COLUMNS_THRESHOLD else Config.TIMESTAMP_WIDTH_NORMAL
                    table_tree.column(col, 
                                    width=timestamp_width, 
                                    minwidth=timestamp_width,
                                    anchor='w')
                else:
                    min_header_width = len(str(col)) * Config.CHAR_WIDTH_MULTIPLIER + Config.HEADER_PADDING
                    final_width = max(int(col_width), min_header_width, Config.MIN_COLUMN_WIDTH)
                    table_tree.column(col, 
                                    width=final_width, 
                                    minwidth=min_header_width, 
                                    anchor='w')
                
            except Exception as e:
                print(f"Error configuring column {col}: {e}")

    def _initialize_lazy_loading(self, table_state: TableState, df):
        """Initialize lazy loading parameters"""
        total_rows = len(df)
        table_state.table_batch_size = Config.TABLE_BATCH_SIZE
        table_state.table_current_batch = 0
        table_state.table_total_batches = (total_rows + table_state.table_batch_size - 1) // table_state.table_batch_size
        
        self.load_table_batch(table_state)

    def _setup_scroll_events(self, table_tree, table_state: TableState, tab_refs: dict):
        """Setup scroll event handling for specific table"""
        self.setup_scroll_event_bindings(table_tree, table_state)
        self.update_table_info_label(table_state, tab_refs)

    def setup_scroll_event_bindings(self, table_tree, table_state: TableState):
        """Setup scroll event detection with proper lambda binding"""
        table_tree.unbind('<MouseWheel>')
        table_tree.unbind('<Button-4>')
        table_tree.unbind('<Button-5>')
        table_tree.unbind('<Key>')
        
        # Use lambda functions for proper event binding
        table_tree.bind('<MouseWheel>', lambda e: self.on_custom_scroll(e, table_state))
        table_tree.bind('<Button-4>', lambda e: self.on_custom_scroll(e, table_state))
        table_tree.bind('<Button-5>', lambda e: self.on_custom_scroll(e, table_state))
        table_tree.bind('<Key>', lambda e: self.on_table_key_scroll(e, table_state))
        
        # Bind to scrollbar events
        for child in table_tree.master.winfo_children():
            if isinstance(child, ttk.Scrollbar) and child.cget('orient') == 'vertical':
                child.bind('<ButtonRelease-1>', lambda e: self.on_scrollbar_release(e, table_state))
                child.bind('<B1-Motion>', lambda e: self.on_scrollbar_drag(e, table_state))
                break
        
        table_state.last_scroll_position = 0.0
        self.schedule_scroll_check(table_state)

    def on_custom_scroll(self, event, table_state: TableState):
        """Handle mouse wheel with custom scroll speed"""
        table_tree = self._get_table_tree_for_state(table_state)
        if not table_tree:
            return
            
        scroll_speed = Config.SCROLL_SPEED
        
        if event.num == 4 or event.delta > 0:
            table_tree.yview_scroll(-scroll_speed, "units")
        elif event.num == 5 or event.delta < 0:
            table_tree.yview_scroll(scroll_speed, "units")
        
        self.root.after(Config.SCROLL_CHECK_DELAY, lambda: self.check_scroll_position(table_state))

    def on_scrollbar_release(self, event, table_state: TableState):
        """Handle scrollbar button release"""
        self.root.after(Config.SCROLLBAR_SETTLE_DELAY, lambda: self.check_scroll_position(table_state))

    def on_scrollbar_drag(self, event, table_state: TableState):
        """Handle scrollbar dragging"""
        self.root.after(Config.SCROLL_CHECK_DELAY, lambda: self.check_scroll_position(table_state))

    def schedule_scroll_check(self, table_state: TableState):
        """Periodically check scroll position for changes"""
        try:
            table_tree = self._get_table_tree_for_state(table_state)
            if table_tree and table_tree.winfo_exists():
                if table_state.all_rows_loaded:
                    print("All rows loaded, stopping scroll monitoring")
                    return
                
                current_position = table_tree.yview()[1]
                
                if abs(current_position - table_state.last_scroll_position) > Config.SCROLL_POSITION_SENSITIVITY:
                    table_state.last_scroll_position = current_position
                    self.check_scroll_position(table_state)
                
                if not table_state.all_rows_loaded:
                    timer_attr = self._get_timer_attr_name(table_state)
                    timer = self.root.after(Config.SCROLL_MONITOR_INTERVAL, lambda: self.schedule_scroll_check(table_state))
                    setattr(self, timer_attr, timer)
            else:
                timer_attr = self._get_timer_attr_name(table_state)
                if hasattr(self, timer_attr) and getattr(self, timer_attr) is not None:
                    self.root.after_cancel(getattr(self, timer_attr))
                    setattr(self, timer_attr, None)
        except Exception as e:
            timer_attr = self._get_timer_attr_name(table_state)
            if hasattr(self, timer_attr) and getattr(self, timer_attr) is not None:
                self.root.after_cancel(getattr(self, timer_attr))
                setattr(self, timer_attr, None)

    def on_table_key_scroll(self, event, table_state: TableState):
        """Handle keyboard scrolling"""
        if event.keysym in ['Down', 'Up', 'Next', 'Prior', 'End']:
            self.root.after(Config.SCROLL_CHECK_DELAY, lambda: self.check_scroll_position(table_state))

    def check_scroll_position(self, table_state: TableState):
        """Check scroll position for lazy loading"""
        try:
            if table_state.all_rows_loaded:
                return
            
            if table_state.table_current_batch >= table_state.table_total_batches:
                return
            
            table_tree = self._get_table_tree_for_state(table_state)
            if not table_tree:
                return
                
            children = table_tree.get_children()
            if not children:
                return
            
            try:
                scroll_top, scroll_bottom = table_tree.yview()
                
                if scroll_bottom > Config.SCROLL_BOTTOM_THRESHOLD:
                    print(f"Scrollbar position: {scroll_bottom:.2f}, loading next batch...")
                    self.load_table_batch(table_state)
                    return
            except Exception as e:
                print(f"Error getting scroll position: {e}")
            
            self._check_visible_items_fallback(table_tree, children, table_state)
                
        except Exception as e:
            print(f"Error checking scroll position: {e}")

    def _check_visible_items_fallback(self, table_tree, children, table_state: TableState):
        """Fallback method to check visible items for scroll detection"""
        try:
            last_visible = None
            
            for i, child in enumerate(children):
                try:
                    bbox = table_tree.bbox(child)
                    if bbox:
                        last_visible = i
                except:
                    continue
            
            if last_visible is not None:
                threshold = len(children) * Config.VISIBLE_ITEMS_THRESHOLD
                if last_visible >= threshold:
                    print(f"Near end of loaded data (item {last_visible}/{len(children)}), loading next batch...")
                    self.load_table_batch(table_state)
                    
        except Exception as e:
            print(f"Error in fallback scroll detection: {e}")

    def load_table_batch(self, table_state: TableState):
        """Load next batch of table data for specific table state"""
        if table_state.current_table_df is None:
            return
        
        if table_state.all_rows_loaded:
            print("All rows already loaded, skipping batch loading")
            return
        
        try:
            df = table_state.current_table_df
            total_rows = len(df)
            
            table_tree = self._get_table_tree_for_state(table_state)
            if not table_tree:
                return
            
            visible_columns = [col for col in df.columns if col not in table_state.hidden_columns]
            total_columns = len(visible_columns)
            
            start_row = table_state.table_current_batch * table_state.table_batch_size
            end_row = min(start_row + table_state.table_batch_size, total_rows)
            
            if start_row >= total_rows:
                return
            
            print(f"Loading batch {table_state.table_current_batch + 1}/{table_state.table_total_batches}: rows {start_row}-{end_row}")
            
            self._insert_batch_rows(table_tree, df, visible_columns, total_columns, start_row, end_row)
            
            table_state.table_current_batch += 1
            
            tab_refs = self._get_tab_refs_for_state(table_state)
            if tab_refs:
                self.update_table_info_label(table_state, tab_refs)
            
        except Exception as e:
            print(f"Error loading table batch: {e}")

    def _get_table_tree_for_state(self, table_state: TableState) -> Optional[ttk.Treeview]:
        """Get table tree widget for specific table state"""
        tab_refs = self._get_tab_refs_for_state(table_state)
        return tab_refs.get('table_tree') if tab_refs else None

    def _get_tab_refs_for_state(self, table_state: TableState) -> Optional[dict]:
        """Get tab references for specific table state"""
        if table_state == self.table1_state:
            return self.table_tabs.get(Config.TAB_TABLE1)
        elif table_state == self.table2_state:
            return self.table_tabs.get(Config.TAB_TABLE2)
        elif table_state == self.search_state:
            return self.table_tabs.get(Config.TAB_SEARCH_RESULTS)
        else:
            return None

    def _insert_batch_rows(self, table_tree, df, visible_columns, total_columns, start_row, end_row):
        """Insert batch of rows into table - OPTIMIZED"""
        # OPTIMIZED: Build all row data first, then insert in bulk
        rows_to_insert = []
        
        for i in range(start_row, end_row):
            row_data = []
            for col in visible_columns:
                try:
                    value = df.iloc[i][col]
                    str_val = self._format_cell_value_for_display(value, col, total_columns)
                    row_data.append(str_val)
                except Exception as e:
                    print(f"Error formatting cell [{i}][{col}]: {e}")
                    row_data.append("Error")
            
            rows_to_insert.append(row_data)
        
        # OPTIMIZED: Insert all rows at once (much faster than individual inserts)
        for row_data in rows_to_insert:
            try:
                table_tree.insert("", "end", values=row_data)
            except Exception as e:
                print(f"Error inserting row: {e}")
                continue

    def _format_cell_value_for_display(self, value, col_name, total_columns):
        """Format cell value for display with appropriate truncation"""
        if isinstance(value, (list, tuple, np.ndarray)):
            str_val = str(value)
        elif pd.isna(value):
            str_val = ""
        else:
            str_val = str(value)
        
        if (total_columns > Config.MANY_COLUMNS_THRESHOLD and 
            col_name.lower() != 'timestamp' and 
            len(str_val) > Config.LONG_VALUE_THRESHOLD):
            str_val = str_val[:Config.TRUNCATE_LENGTH] + Config.TRUNCATE_SUFFIX
        
        return str_val

    def update_table_info_label(self, table_state: TableState, tab_refs: dict):
        """Update the table information label for specific table state"""
        if table_state.current_table_df is None:
            return
            
        try:
            df = table_state.current_table_df
            total_rows = len(df)
            all_columns = list(df.columns)
            visible_columns = [col for col in all_columns if col not in table_state.hidden_columns]
            loaded_rows = min(table_state.table_current_batch * table_state.table_batch_size, total_rows)
            
            df_type_info = self._get_dataframe_type_info(df, table_state)
            column_info = self._get_column_info(visible_columns, table_state)
            batch_info = self._get_batch_info(total_rows, loaded_rows)
            
            info_label = tab_refs.get('info_label')
            if info_label:
                info_label.config(
                    text=f"{table_state.current_table_name}{df_type_info}: {total_rows} rows{column_info}{batch_info}"
                )
        except Exception as e:
            print(f"Error updating table info label: {e}")

    def update_table_info_label_all_loaded(self, table_state: TableState, tab_refs: dict):
        """Update table info when all rows are loaded"""
        if table_state.current_table_df is None:
            return
            
        try:
            df = table_state.current_table_df
            total_rows = len(df)
            all_columns = list(df.columns)
            visible_columns = [col for col in all_columns if col not in table_state.hidden_columns]
            
            df_type_info = self._get_dataframe_type_info(df, table_state)
            column_info = self._get_column_info(visible_columns, table_state)
            
            info_label = tab_refs.get('info_label')
            if info_label:
                info_label.config(
                    text=f"{table_state.current_table_name}{df_type_info}: {Config.INFO_ALL_ROWS_LOADED_FULL.format(total_rows)}{column_info}"
                )
        except Exception as e:
            print(f"Error updating table info label: {e}")

    def _get_dataframe_type_info(self, df, table_state: TableState):
        """Get dataframe type information string"""
        if table_state.current_table_name.endswith('_ALL'):
            return Config.INFO_COMPLETE_DATASET
        elif "Search Results" in table_state.current_table_name:
            return Config.INFO_SEARCH_RESULTS
        elif any(col != 'timestamp' and pd.api.types.is_numeric_dtype(df[col]) for col in df.columns):
            return Config.INFO_NUMERICAL_ONLY
        else:
            return Config.INFO_COMPLETE_DATASET

    def _get_column_info(self, visible_columns, table_state: TableState):
        """Get column information string"""
        total_columns = len(visible_columns) + len(table_state.hidden_columns)
        column_info = f", {total_columns} columns"
        if table_state.hidden_columns:
            column_info += Config.INFO_HIDDEN_COLS.format(len(table_state.hidden_columns))
        
        if len(visible_columns) > Config.MANY_COLUMNS_THRESHOLD:
            column_info += Config.INFO_USE_HSCROLL
        
        return column_info

    def _get_batch_info(self, total_rows, loaded_rows):
        """Get batch loading information string"""
        if total_rows > loaded_rows:
            return Config.INFO_LOADED_ROWS.format(loaded_rows, total_rows)
        else:
            return Config.INFO_ALL_LOADED.format(total_rows)

    def handle_right_click(self, event, table_state: TableState):
        """Handle right-click - determine if header or cell"""
        table_tree = self._get_table_tree_for_state(table_state)
        if not table_tree:
            return
            
        region = table_tree.identify_region(event.x, event.y)
        if region == "heading":
            self.on_table_header_right_click(event, table_state)
        else:
            self.on_table_right_click(event, table_state)

    def on_table_header_right_click(self, event, table_state: TableState):
        """Handle right-click on table headers for column hiding"""
        table_tree = self._get_table_tree_for_state(table_state)
        if not table_tree:
            return
            
        region = table_tree.identify_region(event.x, event.y)
        
        if region == "heading":
            col = table_tree.identify_column(event.x)
            if col:
                try:
                    col_index = int(col.replace('#', '')) - 1
                    if table_state.current_table_df is not None:
                        columns = [c for c in table_state.current_table_df.columns if c not in table_state.hidden_columns]
                        if 0 <= col_index < len(columns):
                            col_name = columns[col_index]
                            self.show_column_context_menu(event, col_name, table_state)
                except:
                    pass
        else:
            self.on_table_right_click(event, table_state)

    def show_column_context_menu(self, event, col_name, table_state: TableState):
        """Show context menu for column operations"""
        context_menu = tk.Menu(self.root, tearoff=0)
        
        if col_name.lower() != 'timestamp':
            context_menu.add_command(label=Config.MENU_HIDE_COLUMN.format(col_name), 
                                command=lambda: self.hide_column(col_name, table_state))
        else:
            context_menu.add_command(label=Config.MENU_CANNOT_HIDE_TIMESTAMP, state='disabled')
        
        context_menu.add_separator()
        
        if table_state.hidden_columns:
            tab_refs = self._get_tab_refs_for_state(table_state)
            context_menu.add_command(label=Config.MENU_SHOW_HIDDEN_COLUMNS, 
                                command=lambda: self.show_column_management_dialog(table_state, tab_refs))
        
        try:
            context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            context_menu.grab_release()

    def hide_column(self, col_name, table_state: TableState):
        """Hide a specific column for specific table state"""
        if col_name.lower() == 'timestamp':
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_CANNOT_HIDE_TIMESTAMP)
            return
        
        table_state.hidden_columns.add(col_name)
        print(f"Hidden column: {col_name}")
               
        tab_refs = self._get_tab_refs_for_state(table_state)
        if tab_refs:
            self.refresh_table_display(table_state, tab_refs)

    def on_table_cell_double_click(self, event, table_state: TableState):
        """Handle double-click on table cells to show full content"""
        table_tree = self._get_table_tree_for_state(table_state)
        if not table_tree:
            return
            
        item = table_tree.identify_row(event.y)
        column = table_tree.identify_column(event.x)
        
        if not item or not column:
            return
        
        table_tree.selection_set(item)
        
        try:
            self._process_cell_double_click(item, column, table_state, table_tree)
        except Exception as e:
            print(f"Error in cell double-click: {e}")
            messagebox.showerror(Config.DIALOG_ERROR, Config.MSG_COULD_NOT_PROCESS.format(str(e)))

    def _process_cell_double_click(self, item, column, table_state: TableState, table_tree):
        """Process cell double-click event with proper error handling"""
        try:
            all_items = table_tree.get_children()
            tree_position = list(all_items).index(item)
            
            col_index = int(column.replace('#', '')) - 1
            visible_columns = [col for col in table_state.current_table_df.columns 
                              if col not in table_state.hidden_columns]
            
            if col_index < 0 or col_index >= len(visible_columns):
                messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_INVALID_COLUMN)
                return
                
            col_name = visible_columns[col_index]
            
            if tree_position >= len(table_state.current_table_df):
                messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_ROW_OUT_OF_BOUNDS.format(tree_position))
                return
            
            original_value = table_state.current_table_df.iloc[tree_position][col_name]
            self.show_cell_content_dialog(col_name, original_value)
            
        except Exception as e:
            print(f"Error in cell double-click processing: {e}")
            messagebox.showerror(Config.DIALOG_ERROR, Config.MSG_COULD_NOT_PROCESS.format(str(e)))

    def on_table_right_click(self, event, table_state: TableState):
        """Handle right-click on table for context menu"""
        table_tree = self._get_table_tree_for_state(table_state)
        if not table_tree:
            return
            
        item = table_tree.identify_row(event.y)
        column = table_tree.identify_column(event.x)
        
        if not item:
            return
        
        table_tree.selection_set(item)
        
        try:
            self._store_right_click_position(item, column, table_state, table_tree)
            self._show_cell_context_menu(event, table_state)
        except (ValueError, IndexError):
            print("Error determining clicked cell position")

    def _store_right_click_position(self, item, column, table_state: TableState, table_tree):
        """Store right-click position for context menu actions"""
        all_items = table_tree.get_children()
        tree_position = list(all_items).index(item)
        self.last_clicked_row = tree_position
        self.last_clicked_column = column
        self.last_clicked_table_state = table_state
        
        col_index = int(column.replace('#', '')) - 1
        visible_columns = [col for col in table_state.current_table_df.columns if col not in table_state.hidden_columns]
        
        if col_index >= 0 and col_index < len(visible_columns):
            self.last_clicked_col_name = visible_columns[col_index]
        else:
            self.last_clicked_col_name = None

    def _show_cell_context_menu(self, event, table_state: TableState):
        """Show context menu for cell operations"""
        context_menu = tk.Menu(self.root, tearoff=0)
        context_menu.add_command(label=Config.MENU_VIEW_FULL_CONTENT, 
                            command=self.show_clicked_cell_content)
        context_menu.add_command(label=Config.MENU_COPY_CELL_VALUE, 
                            command=self.copy_clicked_cell_value)
        
        try:
            context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            context_menu.grab_release()

    def show_clicked_cell_content(self):
        """Show content for the last right-clicked cell"""
        if not hasattr(self, 'last_clicked_row') or not hasattr(self, 'last_clicked_col_name') or not hasattr(self, 'last_clicked_table_state'):
            messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_NO_CELL_SELECTED)
            return
        
        if self.last_clicked_col_name is None:
            messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_INVALID_COLUMN_SELECTED)
            return
        
        actual_df_row = self.last_clicked_row
        col_name = self.last_clicked_col_name
        table_state = self.last_clicked_table_state
        
        try:
            if actual_df_row < len(table_state.current_table_df):
                original_value = table_state.current_table_df.iloc[actual_df_row][col_name]
                self.show_cell_content_dialog(col_name, original_value)
            else:
                messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_ROW_OUT_OF_BOUNDS.format(actual_df_row))
        except Exception as e:
            messagebox.showerror(Config.DIALOG_ERROR, Config.MSG_COULD_NOT_RETRIEVE.format(str(e)))

    def copy_clicked_cell_value(self):
        """Copy value for the last right-clicked cell"""
        if not hasattr(self, 'last_clicked_row') or not hasattr(self, 'last_clicked_col_name') or not hasattr(self, 'last_clicked_table_state'):
            messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_NO_CELL_SELECTED)
            return
        
        if self.last_clicked_col_name is None:
            messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_INVALID_COLUMN_SELECTED)
            return
        
        actual_df_row = self.last_clicked_row
        col_name = self.last_clicked_col_name
        table_state = self.last_clicked_table_state
        
        try:
            if actual_df_row < len(table_state.current_table_df):
                original_value = table_state.current_table_df.iloc[actual_df_row][col_name]
                
                if isinstance(original_value, (list, tuple, np.ndarray)):
                    copy_text = ', '.join(map(str, original_value))
                else:
                    copy_text = str(original_value)
                
                self.root.clipboard_clear()
                self.root.clipboard_append(copy_text)
            else:
                messagebox.showwarning(Config.DIALOG_ERROR, Config.MSG_ROW_OUT_OF_BOUNDS.format(actual_df_row))
        except Exception as e:
            messagebox.showerror(Config.DIALOG_ERROR, Config.MSG_COULD_NOT_COPY.format(str(e)))

    def export_current_table_to_csv(self, table_state: TableState):
        """Export current table data to CSV for specific table state"""
        if table_state.current_table_df is None:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_NO_TABLE_DATA)
            return
        
        try:
            safe_filename = self.sanitize_filename(table_state.current_table_name)
            if not safe_filename:
                safe_filename = Config.DEFAULT_EXPORT_NAME
            
            initial_filename = f"{safe_filename}.csv"
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=Config.CSV_EXTENSIONS,
                title="Save CSV file",
                initialfile=initial_filename,
                parent=self.root
            )
            
            if file_path:
                table_state.current_table_df.to_csv(file_path, index=False)
                messagebox.showinfo(Config.DIALOG_SUCCESS, Config.MSG_EXPORT_SUCCESS.format(file_path))
            
        except Exception as e:
            messagebox.showerror(Config.DIALOG_EXPORT_ERROR, Config.MSG_EXPORT_FAILED.format(str(e)))

    def show_progress_dialog(self, title, max_value):
        """Show a simple progress dialog"""
        class ProgressDialog:
            def __init__(self, parent, title, max_value):
                self.dialog = tk.Toplevel(parent)
                self.dialog.title(title)
                self.dialog.geometry(Config.PROGRESS_DIALOG_SIZE)
                self.dialog.transient(parent)
                self.dialog.grab_set()
                
                self.dialog.geometry("+%d+%d" % (parent.winfo_rootx() + 200, parent.winfo_rooty() + 200))
                
                self.progress = ttk.Progressbar(self.dialog, length=Config.PROGRESS_BAR_LENGTH, mode='determinate')
                self.progress.pack(pady=20)
                self.progress['maximum'] = max_value
                
                self.status_label = tk.Label(self.dialog, text="Starting...")
                self.status_label.pack(pady=10)
            
            def update_progress(self, value, text):
                try:
                    self.progress['value'] = value
                    self.status_label.config(text=text)
                except:
                    pass
            
            def close(self):
                try:
                    self.dialog.destroy()
                except:
                    pass
        
        return ProgressDialog(self.root, title, max_value)

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename by removing invalid characters"""
        if not filename or filename.isspace():
            return Config.DEFAULT_EXPORT_NAME
        
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        sanitized = sanitized.strip('. ')
        sanitized = re.sub(r'_+', '_', sanitized)
        sanitized = sanitized.strip('_')
        
        return sanitized if sanitized else Config.DEFAULT_EXPORT_NAME

    def load_log_file(self):
        """Load and parse log file"""
        try:
            raw_dataframes, filename = parse_log_file()
            
            if not raw_dataframes:
                return
            
            self.clear_plot()
            self._clear_all_tables()
            
            self.current_log_filename = filename
            self.update_title_bar()
            
            self.pandas_dfs = self.split_mixed_dataframes(raw_dataframes)
            self.polars_dfs = convert_to_polars(self.pandas_dfs)
            
            self.plotted_variables.clear()
            self.plotted_variables_right.clear()
            
            self.populate_variable_tree()
            
            messagebox.showinfo(Config.DIALOG_SUCCESS, Config.MSG_LOAD_SUCCESS)
            
        except Exception as e:
            messagebox.showerror(Config.DIALOG_ERROR, Config.MSG_LOAD_FAILED.format(str(e)))

    def _clear_all_tables(self):
        """Clear all table tabs"""
        # Clear each table state
        for state in [self.table1_state, self.table2_state, self.search_state]:
            state.current_table_df = None
            state.current_table_name = ""
            state.reset_for_new_dataframe()
        
        self.current_search_result = None
        
        # Use state-based clearing
        for state, tab_name in [(self.table1_state, Config.TAB_TABLE1), 
                               (self.table2_state, Config.TAB_TABLE2), 
                               (self.search_state, Config.TAB_SEARCH_RESULTS)]:
            tab_refs = self.table_tabs.get(tab_name)
            if tab_refs:
                table_tree = tab_refs.get('table_tree')
                info_label = tab_refs.get('info_label')
                
                if table_tree:
                    for item in table_tree.get_children():
                        table_tree.delete(item)
                    table_tree["columns"] = ()
                    table_tree["show"] = "tree"
                
                if info_label:
                    if state == self.search_state:
                        info_label.config(text=Config.LABEL_NO_SEARCH)
                    else:
                        info_label.config(text=Config.LABEL_NO_DATA)
        
        if hasattr(self, 'search_details_label'):
            self.search_details_label.config(text=Config.LABEL_SEARCH_PLACEHOLDER)

    def update_title_bar(self):
        """Update the title bar with current filename"""
        if self.current_log_filename:
            title = f"{self.root.app_title} - {self.current_log_filename}"
        else:
            title = self.root.app_title
        
        self.root.title(title)

    def split_mixed_dataframes(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Split DataFrames into numerical (for plotting) and complete _ALL (for table viewing)"""
        split_dataframes = {}
        
        for df_name, df in dataframes.items():
            if len(df) == 0:
                split_dataframes[df_name] = df
                continue
            
            numerical_cols, non_numerical_cols = self._categorize_columns(df)
            
            if numerical_cols and non_numerical_cols:
                numerical_df = df[['timestamp'] + numerical_cols].copy()
                split_dataframes[df_name] = numerical_df
                
                complete_df = df.copy()
                split_dataframes[f"{df_name}_ALL"] = complete_df
                
                print(f"Split {df_name}:")
                print(f"  {df_name}: timestamp + {len(numerical_cols)} numerical columns (for plotting)")
                print(f"  {df_name}_ALL: timestamp + {len(df.columns)-1} total columns (for table viewing)")
                
            else:
                split_dataframes[df_name] = df
                if numerical_cols:
                    print(f"Kept {df_name}: purely numerical ({len(numerical_cols)} columns)")
                elif non_numerical_cols:
                    print(f"Kept {df_name}: purely non-numerical ({len(non_numerical_cols)} columns)")
        
        return split_dataframes

    def _categorize_columns(self, df):
        """Categorize columns into numerical and non-numerical"""
        numerical_cols = []
        non_numerical_cols = []
        
        for col in df.columns:
            if col == 'timestamp':
                continue
            
            if pd.api.types.is_numeric_dtype(df[col]):
                non_null_data = df[col].dropna()
                if len(non_null_data) > 0:
                    if non_null_data.nunique() > 1:
                        unique_vals = set(non_null_data.unique())
                        if not (unique_vals == {0} or unique_vals == {1} or unique_vals == {0, 1}):
                            numerical_cols.append(col)
                        else:
                            non_numerical_cols.append(col)
                    else:
                        non_numerical_cols.append(col)
                else:
                    non_numerical_cols.append(col)
            else:
                non_numerical_cols.append(col)
        
        return numerical_cols, non_numerical_cols

    def populate_variable_tree(self):
        """Populate the variable selection tree"""
        for item in self.variable_tree.get_children():
            self.variable_tree.delete(item)
        
        for df_name, df in self.pandas_dfs.items():
            if len(df) == 0 or df_name.endswith('_ALL'):
                continue
                
            parent = self.variable_tree.insert("", "end", text=df_name, 
                                            values=("DataFrame", len(df), ""))
            
            ordered_columns = self._get_ordered_columns(df_name, df)
            self._populate_tree_columns(parent, df_name, ordered_columns)

    def _get_ordered_columns(self, df_name, df):
        """Get columns in original order from reference dataframe"""
        all_df_name = f"{df_name}_ALL"
        if all_df_name in self.pandas_dfs:
            reference_df = self.pandas_dfs[all_df_name]
            print(f"  Using {all_df_name} as column order reference")
        else:
            reference_df = df
        
        ordered_columns = []
        for col in reference_df.columns:
            if col == 'timestamp':
                continue
                
            is_numeric = False
            
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                is_numeric = True
            
            elif (all_df_name in self.pandas_dfs and 
                col in self.pandas_dfs[all_df_name].columns and 
                pd.api.types.is_numeric_dtype(self.pandas_dfs[all_df_name][col])):
                is_numeric = True
                if col not in df.columns:
                    print(f"    Found additional numeric column: {col}")
            
            if is_numeric:
                ordered_columns.append(col)
        
        return ordered_columns

    def _populate_tree_columns(self, parent, df_name, ordered_columns):
        """Populate tree with columns in correct order"""
        all_df_name = f"{df_name}_ALL"
        
        for col in ordered_columns:
            if col in self.pandas_dfs[df_name].columns:
                source_df = self.pandas_dfs[df_name]
            elif all_df_name in self.pandas_dfs and col in self.pandas_dfs[all_df_name].columns:
                source_df = self.pandas_dfs[all_df_name]
            else:
                continue
            
            non_null_count = source_df[col].notna().sum()
            dtype = str(source_df[col].dtype)
            full_name = f"{df_name}.{col}"
            
            self.variable_tree.insert(parent, "end", 
                                    text=col,
                                    values=(dtype, non_null_count, full_name))
        
        print(f"  {df_name}: {len(ordered_columns)} columns in original dataframe order")
    
    def get_full_name_from_item(self, item) -> Optional[str]:
        """Get the full variable name from a tree item"""
        try:
            values = self.variable_tree.item(item, "values")
            if len(values) >= 3 and values[2]:
                return values[2]
            return None
        except:
            return None
    
    def get_selected_variables(self) -> List[str]:
        """Get list of selected variables from tree"""
        selected = []
        
        for sel_item in self.variable_tree.selection():
            full_name = self.get_full_name_from_item(sel_item)
            if full_name and "." in full_name:
                selected.append(full_name)
        
        return selected
    
    def highlight_plotted_variable(self, var_name: str, axis: str = "left"):
        """Highlight a variable in the tree as plotted"""
        tag = "plotted_left" if axis == "left" else "plotted_right"
        
        def find_and_highlight(item):
            full_name = self.get_full_name_from_item(item)
            if full_name == var_name:
                self.variable_tree.item(item, tags=(tag,))
                return True
            
            for child in self.variable_tree.get_children(item):
                if find_and_highlight(child):
                    return True
            return False
        
        for item in self.variable_tree.get_children():
            find_and_highlight(item)
    
    def remove_highlight(self, var_name: str):
        """Remove highlighting from a specific variable"""
        def find_and_remove_highlight(item):
            full_name = self.get_full_name_from_item(item)
            if full_name == var_name:
                self.variable_tree.item(item, tags=())
                return True
            
            for child in self.variable_tree.get_children(item):
                if find_and_remove_highlight(child):
                    return True
            return False
        
        for item in self.variable_tree.get_children():
            find_and_remove_highlight(item)
    
    def remove_all_highlights(self):
        """Remove highlighting from all variables"""
        def remove_highlight(item):
            self.variable_tree.item(item, tags=())
            for child in self.variable_tree.get_children(item):
                remove_highlight(child)
        
        for item in self.variable_tree.get_children():
            remove_highlight(item)
    
    def create_secondary_axis(self):
        """Create secondary y-axis if it doesn't exist"""
        if self.ax2 is None:
            self.ax2 = self.ax.twinx()
            self.ax2.tick_params(axis='y', labelcolor=Config.RIGHT_AXIS_COLOR)

    def reset_right_axis_limits(self):
        """Reset right y-axis limits based on current data"""
        if self.ax2 is not None and len(self.ax2.get_lines()) > 0:
            all_y_data = []
            for line in self.ax2.get_lines():
                y_data = line.get_ydata()
                if len(y_data) > 0:
                    all_y_data.extend(y_data)
            
            if all_y_data:
                min_val = min(all_y_data)
                max_val = max(all_y_data)
                margin = (max_val - min_val) * Config.AXIS_MARGIN_FACTOR if max_val != min_val else abs(max_val) * Config.FALLBACK_MARGIN_FACTOR
                self.ax2.set_ylim(min_val - margin, max_val + margin)
    
    def remove_plot_line(self, var_name: str, axis: str = "left"):
        """Remove a specific plot line from the graph"""
        target_ax = self.ax if axis == "left" else self.ax2
        if target_ax is None:
            return
        
        search_labels = [var_name]
        if axis == "right":
            search_labels.append(f"[R] {var_name}")
        
        lines_to_remove = []
        for line in target_ax.get_lines():
            if line.get_label() in search_labels:
                lines_to_remove.append(line)
        
        for line in lines_to_remove:
            line.remove()
    
    def plot_variable(self, var_name: str, axis: str = "left"):
        """Plot a single variable on specified axis"""
        try:
            df_name, col_name = var_name.split(".", 1)
            
            df = self._get_dataframe_for_plotting(df_name, col_name)
            if df is None:
                return False
            
            mask = df[col_name].notna() & df['timestamp'].notna()
            x_data = df.loc[mask, 'timestamp']
            y_data = df.loc[mask, col_name]
            
            if len(x_data) > 0:
                success = self._plot_data_on_axis(x_data, y_data, var_name, axis)
                if success:
                    self._update_plotted_variables(var_name, axis)
                    self.highlight_plotted_variable(var_name, axis)
                return success
                
        except Exception as e:
            print(f"Error plotting {var_name}: {e}")
            return False
        return False

    def _get_dataframe_for_plotting(self, df_name, col_name):
        """Get appropriate dataframe for plotting"""
        if df_name in self.pandas_dfs and col_name in self.pandas_dfs[df_name].columns:
            return self.pandas_dfs[df_name]
        elif f"{df_name}_ALL" in self.pandas_dfs and col_name in self.pandas_dfs[f"{df_name}_ALL"].columns:
            print(f"Plotting {col_name} from {df_name}_ALL dataframe")
            return self.pandas_dfs[f"{df_name}_ALL"]
        else:
            print(f"Column {col_name} not found in {df_name} or {df_name}_ALL")
            return None

    def _plot_data_on_axis(self, x_data, y_data, var_name, axis):
        """Plot data on the specified axis"""
        target_ax, colors, color_idx = self._get_plot_settings(axis)
        legend_name = f"[R] {var_name}" if axis == "right" else var_name
        
        if len(x_data) == 1:
            target_ax.plot(x_data, y_data, 'o', label=legend_name,
                        color=colors[color_idx], markersize=Config.MARKER_SIZE, alpha=Config.ALPHA)
        else:
            target_ax.plot(x_data, y_data, label=legend_name,
                        color=colors[color_idx], linewidth=Config.LINE_WIDTH, alpha=Config.ALPHA)
        
        if axis == "right" and len(target_ax.get_lines()) == 1:
            self.reset_right_axis_limits()
        
        return True

    def _get_plot_settings(self, axis):
        """Get plot settings for axis"""
        if axis == "right":
            self.create_secondary_axis()
            target_ax = self.ax2
            colors = plt.cm.Set2(np.linspace(0, 1, Config.RIGHT_COLORMAP_SIZE))
            existing_lines = len(target_ax.get_lines())
            color_idx = existing_lines % Config.RIGHT_COLORMAP_SIZE
        else:
            target_ax = self.ax
            colors = plt.cm.tab10(np.linspace(0, 1, Config.LEFT_COLORMAP_SIZE))
            existing_lines = len(target_ax.get_lines())
            color_idx = existing_lines % Config.LEFT_COLORMAP_SIZE
        
        return target_ax, colors, color_idx

    def _update_plotted_variables(self, var_name, axis):
        """Update plotted variables sets"""
        if axis == "right":
            self.plotted_variables_right.add(var_name)
        else:
            self.plotted_variables.add(var_name)
    
    def unplot_variable(self, var_name: str):
        """Remove a variable from the plot (detect which axis it's on)"""
        removed = False
        
        if var_name in self.plotted_variables:
            self.remove_plot_line(var_name, "left")
            self.plotted_variables.remove(var_name)
            removed = True
        
        if var_name in self.plotted_variables_right:
            self.remove_plot_line(var_name, "right")
            self.plotted_variables_right.remove(var_name)
            removed = True
            
            if self.ax2 is not None and len(self.ax2.get_lines()) == 0:
                self.ax2.relim()
                self.ax2.autoscale()
        
        if removed:
            self.remove_highlight(var_name)
            self.update_plot_appearance()
        
        return removed
    
    def plot_selected(self):
        """Plot selected variables on left axis"""
        if not self.pandas_dfs:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_NO_LOG_FILE)
            return
        
        selected_vars = self.get_selected_variables()
        
        if not selected_vars:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_SELECT_VARIABLES)
            return
        
        self.notebook.select(self.plot_frame)
        
        any_plotted = False
        for var_name in selected_vars:
            if self.plot_variable(var_name, "left"):
                any_plotted = True
        
        if any_plotted:
            self.update_plot_appearance()

    def update_plot_appearance(self):
        """Update plot appearance and refresh"""
        self._configure_main_plot()
        self._configure_right_axis()
        self._update_axis_scales()
        self._update_legend()
        self._finalize_plot_layout()

    def _configure_main_plot(self):
        """Configure main plot settings"""
        self.ax.set_xlabel('Time')
        self.ax.grid(True, alpha=Config.GRID_ALPHA)
        self.ax.tick_params(axis='y', labelcolor=Config.LEFT_AXIS_COLOR)
        
        if len(self.ax.get_lines()) > 0:
            self.ax.relim()
            self.ax.autoscale()

    def _configure_right_axis(self):
        """Configure right axis if it exists"""
        if self.ax2 is not None:
            self.ax2.tick_params(axis='y', labelcolor=Config.RIGHT_AXIS_COLOR)
            
            if len(self.ax2.get_lines()) > 0:
                self.ax2.relim()
                self.ax2.autoscale()
            else:
                self.ax2.set_ylim(0, 1)

    def _update_axis_scales(self):
        """Update axis scales based on settings"""
        scale = self.y_scale_var.get()
        if scale == "log":
            self.ax.set_yscale('log')
            if self.ax2 is not None and len(self.ax2.get_lines()) > 0:
                self.ax2.set_yscale('log')
        else:
            self.ax.set_yscale('linear')
            if self.ax2 is not None:
                self.ax2.set_yscale('linear')

    def _update_legend(self):
        """Update combined legend for both axes"""
        lines1, labels1 = self.ax.get_legend_handles_labels()
        lines2, labels2 = [], []
        if self.ax2 is not None:
            lines2, labels2 = self.ax2.get_legend_handles_labels()
        
        if lines1 or lines2:
            all_lines = lines1 + lines2
            all_labels = labels1 + labels2
            
            legend = self.ax.legend(all_lines, all_labels, 
                                    bbox_to_anchor=Config.LEGEND_BBOX, 
                                    loc=Config.LEGEND_LOCATION,
                                    fontsize=Config.LEGEND_FONTSIZE,
                                    frameon=True,
                                    fancybox=True,
                                    shadow=False,
                                    ncol=1,
                                    borderpad=Config.LEGEND_BORDERPAD,
                                    columnspacing=Config.LEGEND_COLUMNSPACING,
                                    handlelength=Config.LEGEND_HANDLELENGTH,
                                    handletextpad=Config.LEGEND_HANDLETEXTPAD)
        else:
            legend = self.ax.get_legend()
            if legend:
                legend.remove()

    def _finalize_plot_layout(self):
        """Finalize plot layout and refresh"""
        self.fig.tight_layout(rect=[0, 0, 1, 1])
        self.canvas.draw()

    def refresh_plot(self):
        """Refresh the current plot with new settings"""
        self.update_plot_appearance()
    
    def clear_plot(self):
        """Clear the plot and remove highlights"""
        self.ax.clear()
        if self.ax2 is not None:
            self.ax2.remove()
            self.ax2 = None
        
        self.ax.set_xlabel('Time')
        self.ax.grid(True, alpha=Config.GRID_ALPHA)
        self.ax.tick_params(axis='y', labelcolor=Config.LEFT_AXIS_COLOR)
        self.canvas.draw()
        
        self.plotted_variables.clear()
        self.plotted_variables_right.clear()
        self.remove_all_highlights()       
    
    def on_tree_double_click(self, event):
        """Handle double-click on tree items - LEFT CLICK opens in Table-1"""
        item = self.variable_tree.selection()[0] if self.variable_tree.selection() else None
        if not item:
            return
        
        full_name = self.get_full_name_from_item(item)
        item_text = self.variable_tree.item(item, "text")
        
        if full_name and "." in full_name:
            self._handle_variable_double_click(full_name)
        elif "." not in item_text and item_text in self.pandas_dfs:
            self._handle_dataframe_double_click(item_text, target_table=Config.TAB_TABLE1)

    def _handle_variable_double_click(self, full_name):
        """Handle double-click on variable"""
        if not self.pandas_dfs:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_NO_LOG_FILE)
            return
        
        self.notebook.select(self.plot_frame)
        
        if full_name in self.plotted_variables or full_name in self.plotted_variables_right:
            self.unplot_variable(full_name)
        else:
            if self.plot_variable(full_name, "left"):
                self.update_plot_appearance()

    def _handle_dataframe_double_click(self, item_text, target_table=Config.TAB_TABLE1):
        """Handle double-click on dataframe - specify target table"""
        all_version_name = f"{item_text}_ALL"
        if all_version_name in self.pandas_dfs:
            df_name = all_version_name
        else:
            df_name = item_text
        
        if target_table == Config.TAB_TABLE1:
            self.show_dataframe_in_table(df_name, self.table1_state, Config.TAB_TABLE1)
        elif target_table == Config.TAB_TABLE2:
            self.show_dataframe_in_table(df_name, self.table2_state, Config.TAB_TABLE2)
    
    def on_tree_right_double_click(self, event):
        """Handle RIGHT DOUBLE-CLICK on tree items - Variables to right axis, DataFrames to Table-2"""
        item = self.variable_tree.identify_row(event.y)
        if not item:
            return
        
        self.variable_tree.selection_set(item)
        
        full_name = self.get_full_name_from_item(item)
        item_text = self.variable_tree.item(item, "text")
        
        if full_name and "." in full_name:
            self._handle_variable_right_double_click(full_name)
        elif "." not in item_text and item_text in self.pandas_dfs:
            self._handle_dataframe_double_click(item_text, target_table=Config.TAB_TABLE2)

    def _handle_variable_right_double_click(self, full_name):
        """Handle right double-click on variable"""
        if not self.pandas_dfs:
            messagebox.showwarning(Config.DIALOG_WARNING, Config.MSG_NO_LOG_FILE)
            return
        
        self.notebook.select(self.plot_frame)
        
        if full_name in self.plotted_variables or full_name in self.plotted_variables_right:
            self.unplot_variable(full_name)
        else:
            if self.plot_variable(full_name, "right"):
                self.update_plot_appearance()

    def show_dataframe_in_table(self, df_name: str, table_state: TableState, tab_name: str):
        """Display DataFrame in the specified table tab - OPTIMIZED"""
        if df_name not in self.pandas_dfs:
            return
        
        # FAST PATH: Check if same dataframe is already displayed
        same_dataframe = (df_name == table_state.current_table_name)
        has_data = table_state.current_table_df is not None
        
        if same_dataframe and has_data:
            # INSTANT: Just switch tab, data is already there!
            print(f"Fast switch: {df_name} already displayed in {tab_name}")
            
            if table_state == self.table1_state:
                self.notebook.select(self.table1_frame)
            elif table_state == self.table2_state:
                self.notebook.select(self.table2_frame)
            elif table_state == self.search_state:
                self.notebook.select(self.search_frame)
            
            return  # Done! Instant switch.
        
        # SLOW PATH: Need to load the table
        print(f"Loading {df_name} into {tab_name}")
        
        if df_name != table_state.current_table_name:
            table_state.reset_for_new_dataframe()
        
        df = self.pandas_dfs[df_name]
        table_state.current_table_df = df
        table_state.current_table_name = df_name
        
        # Switch to the tab
        if table_state == self.table1_state:
            self.notebook.select(self.table1_frame)
        elif table_state == self.table2_state:
            self.notebook.select(self.table2_frame)
        elif table_state == self.search_state:
            self.notebook.select(self.search_frame)
        
        # Load the table
        tab_refs = self.table_tabs[tab_name]
        self.refresh_table_display(table_state, tab_refs)

    def calculate_column_width(self, series: pd.Series, col_name: str, total_columns: int = 1) -> int:
        """Calculate column width"""
        try:
            if col_name.lower() == 'timestamp':
                if total_columns > Config.MANY_COLUMNS_THRESHOLD:
                    return Config.TIMESTAMP_WIDTH_MANY_COLS
                else:
                    return Config.TIMESTAMP_WIDTH_NORMAL
            
            header_width = len(str(col_name)) * Config.CHAR_WIDTH_MULTIPLIER + Config.HEADER_PADDING
            
            if total_columns > Config.MANY_COLUMNS_THRESHOLD:
                base_width = max(header_width, Config.MIN_COLUMN_WIDTH)
                
                sample_size = min(Config.COLUMN_SAMPLE_SIZE_SMALL, len(series))
                if sample_size > 0:
                    sample_data = series.dropna().head(sample_size)
                    if len(sample_data) > 0:
                        try:
                            max_length = max(len(str(val)) for val in sample_data)
                            content_width = min(max_length * 5, Config.MIN_COLUMN_WIDTH)
                            return max(base_width, content_width)
                        except Exception as e:
                            print(f"Error calculating max length: {e}")
                            return base_width
                return base_width
            
            else:
                base_width = max(header_width, Config.MIN_COLUMN_WIDTH)
                
                sample_size = min(Config.COLUMN_SAMPLE_SIZE_LARGE, len(series))
                if sample_size > 0:
                    sample_data = series.dropna().head(sample_size)
                    if len(sample_data) > 0:
                        try:
                            max_length = max(len(str(val)) for val in sample_data)
                            content_width = min(max_length * 7, Config.MAX_COLUMN_WIDTH)
                            return max(base_width, content_width)
                        except Exception as e:
                            print(f"Error calculating max length: {e}")
                            return base_width
                return base_width
        
        except Exception as e:
            print(f"Error in calculate_column_width for {col_name}: {e}")
            return max(len(str(col_name)) * Config.CHAR_WIDTH_MULTIPLIER + Config.HEADER_PADDING, Config.MIN_COLUMN_WIDTH)

    def show_cell_content_dialog(self, column_name: str, value):
        """Show a dialog with the full cell content"""
        dialog = self._create_cell_content_dialog(column_name)
        text_widget = self._setup_cell_content_display(dialog)
        content = self._format_cell_content(value)
        self._finalize_cell_content_dialog(dialog, text_widget, content)

    def _create_cell_content_dialog(self, column_name):
        """Create cell content dialog window"""
        dialog = tk.Toplevel(self.root)
        dialog.title(Config.DIALOG_FULL_CONTENT.format(column_name))
        dialog.geometry(Config.CELL_CONTENT_DIALOG_SIZE)
        dialog.transient(self.root)
        dialog.grab_set()
        return dialog

    def _setup_cell_content_display(self, dialog):
        """Setup cell content display area"""
        content_frame = ttk.Frame(dialog)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        text_frame = ttk.Frame(content_frame)
        text_frame.pack(fill=tk.BOTH, expand=True)
        
        text_widget = tk.Text(text_frame, wrap=tk.WORD, font=("Consolas", 10))
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        v_scroll = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=text_widget.yview)
        v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        text_widget.configure(yscrollcommand=v_scroll.set)
        
        h_scroll = ttk.Scrollbar(content_frame, orient=tk.HORIZONTAL, command=text_widget.xview)
        h_scroll.pack(fill=tk.X)
        text_widget.configure(xscrollcommand=h_scroll.set)
        
        return text_widget

    def _format_cell_content(self, value):
        """Format cell content for display with appropriate handling"""
        content = ""
        
        try:
            if value is None:
                content = "(None value)"
            elif isinstance(value, str) and value.strip() == "":
                content = "(Empty string)"
            elif isinstance(value, (list, tuple)):
                content = self._format_list_content(value)
            elif hasattr(value, '__len__') and hasattr(value, '__iter__') and not isinstance(value, str):
                content = self._format_array_content(value)
            else:
                content = self._format_scalar_content(value)
                    
        except Exception as outer_error:
            content = f"Error processing value: {outer_error}\n\n"
            content += f"Value type: {type(value)}\n"
            content += f"Raw representation: {repr(value)}"
        
        return content

    def _format_list_content(self, value):
        """Format list/tuple content"""
        content = f"List/Tuple content ({type(value).__name__}):\n\n"
        content += f"Length: {len(value)}\n\n"
        
        if len(value) == 0:
            content += "(Empty list)"
        else:
            content += "Elements:\n"
            for i in range(len(value)):
                try:
                    element = value[i]
                    content += f"[{i}]: {element}\n"
                except:
                    content += f"[{i}]: <error reading element>\n"
            
            try:
                comma_separated = ", ".join(str(item) for item in value)
                content += f"\nComma-separated:\n{comma_separated}"
            except:
                content += f"\nRaw representation:\n{str(value)}"
            
            content += self._calculate_numeric_statistics(value)
        
        return content

    def _format_array_content(self, value):
        """Format array-like content"""
        content = f"Array-like object ({type(value).__name__}):\n\n"
        try:
            as_list = []
            for item in value:
                as_list.append(item)
            
            content += f"Length: {len(as_list)}\n\n"
            
            if len(as_list) == 0:
                content += "(Empty array)"
            else:
                content += "Elements:\n"
                for i, item in enumerate(as_list):
                    content += f"[{i}]: {item}\n"
                
                comma_separated = ", ".join(str(item) for item in as_list)
                content += f"\nComma-separated:\n{comma_separated}"
                
        except Exception as conversion_error:
            content += f"Could not convert to list: {conversion_error}\n"
            content += f"Raw string representation:\n{str(value)}"
        
        return content

    def _format_scalar_content(self, value):
        """Format scalar content"""
        try:
            if isinstance(value, float) and math.isnan(value):
                return "(NaN value)"
            else:
                return str(value)
        except:
            return str(value)

    def _calculate_numeric_statistics(self, value):
        """Calculate numeric statistics for list values"""
        try:
            numeric_count = 0
            numeric_sum = 0.0
            min_val = None
            max_val = None
            
            for item in value:
                try:
                    num_val = float(str(item))
                    numeric_count += 1
                    numeric_sum += num_val
                    if min_val is None or num_val < min_val:
                        min_val = num_val
                    if max_val is None or num_val > max_val:
                        max_val = num_val
                except:
                    continue
            
            if numeric_count > 0:
                stats = f"\n\nNumeric Statistics ({numeric_count}/{len(value)} elements):"
                stats += f"\n  Min: {min_val}"
                stats += f"\n  Max: {max_val}"
                stats += f"\n  Average: {numeric_sum/numeric_count:.3f}"
                stats += f"\n  Sum: {numeric_sum}"
                return stats
            else:
                return ""
                
        except Exception as stats_error:
            return f"\n\n(Could not calculate statistics: {stats_error})"

    def _finalize_cell_content_dialog(self, dialog, text_widget, content):
        """Finalize cell content dialog setup"""
        text_widget.insert(tk.END, content)
        text_widget.configure(state=tk.DISABLED)
        
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(button_frame, text=Config.BTN_COPY_CLIPBOARD, 
                command=lambda: self.copy_to_clipboard(content)).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text=Config.BTN_CLOSE, 
                command=dialog.destroy).pack(side=tk.RIGHT, padx=5)

    def copy_to_clipboard(self, content: str):
        """Copy content to clipboard"""
        self.root.clipboard_clear()
        self.root.clipboard_append(content)

    def run(self):
        """Start the GUI"""
        self.root.mainloop()

def main():
    app = LogDataPlotter()
    app.run()

if __name__ == "__main__":
    main()