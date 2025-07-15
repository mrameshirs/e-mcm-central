# css_styles.py
import streamlit as st

def load_custom_css():
    st.markdown("""
    <style>
        /* --- Global Styles --- */
        body {
            font-family: 'Roboto', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #eef2f7;
            color: #4A4A4A;
            line-height: 1.6;
        }
        .stApp {
             background: linear-gradient(135deg, #f0f7ff 0%, #cfe7fa 100%);
        }

        /* --- Titles and Headers --- */
        .page-main-title {
            font-size: 3em;
            color: #1A237E;
            text-align: center;
            padding: 30px 0 10px 0;
            font-weight: 700;
            letter-spacing: 1.5px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }
        .page-app-subtitle {
            font-size: 1.3em;
            color: #3F51B5;
            text-align: center;
            margin-top: -5px;
            margin-bottom: 30px;
            font-weight: 400;
        }
        .app-description {
            font-size: 1.0em;
            color: #455A64;
            text-align: center;
            margin-bottom: 25px;
            padding: 0 20px;
            max-width: 700px;
            margin-left: auto;
            margin-right: auto;
        }
        .sub-header {
            font-size: 1.6em;
            color: #2779bd;
            border-bottom: 3px solid #5dade2;
            padding-bottom: 12px;
            margin-top: 35px;
            margin-bottom: 25px;
            font-weight: 600;
        }
        .card h3 {
            margin-top: 0;
            color: #1abc9c;
            font-size: 1.3em;
            font-weight: 600;
        }
         .card h4 {
            color: #2980b9;
            font-size: 1.1em;
            margin-top: 15px;
            margin-bottom: 8px;
        }

        /* --- Cards --- */
        .card {
            background-color: #ffffff;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 6px 12px rgba(0,0,0,0.08);
            margin-bottom: 25px;
            border-left: 6px solid #5dade2;
        }

        /* --- Streamlit Widgets Styling --- */
        .stButton>button {
            border-radius: 25px;
            background-image: linear-gradient(to right, #1abc9c 0%, #16a085 100%);
            color: white;
            padding: 12px 24px;
            font-weight: bold;
            border: none;
            transition: all 0.3s ease;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stButton>button:hover {
            background-image: linear-gradient(to right, #16a085 0%, #1abc9c 100%);
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }
        .stButton>button[kind="secondary"] {
            background-image: linear-gradient(to right, #e74c3c 0%, #c0392b 100%);
        }
        .stButton>button[kind="secondary"]:hover {
            background-image: linear-gradient(to right, #c0392b 0%, #e74c3c 100%);
        }
        .stButton>button:disabled {
            background-image: none;
            background-color: #bdc3c7;
            color: #7f8c8d;
            box-shadow: none;
            transform: none;
        }
        .stTextInput>div>div>input, .stSelectbox>div>div>div, .stDateInput>div>div>input, .stNumberInput>div>div>input {
            border-radius: 8px;
            border: 1px solid #ced4da;
            padding: 10px;
        }
        .stTextInput>div>div>input:focus, .stSelectbox>div>div>div:focus-within, .stNumberInput>div>div>input:focus {
            border-color: #5dade2;
            box-shadow: 0 0 0 0.2rem rgba(93, 173, 226, 0.25);
        }
        .stFileUploader>div>div>button {
            border-radius: 25px;
            background-image: linear-gradient(to right, #5dade2 0%, #2980b9 100%);
            color: white;
            padding: 10px 18px;
        }
        .stFileUploader>div>div>button:hover {
            background-image: linear-gradient(to right, #2980b9 0%, #5dade2 100%);
        }

        /* --- Login Page Specific --- */
        .login-form-container {
            max-width: 500px;
            margin: 20px auto;
            padding: 30px;
            background-color: #ffffff;
            border-radius: 15px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.1);
        }
        .login-form-container .stButton>button {
            background-image: linear-gradient(to right, #34495e 0%, #2c3e50 100%);
        }
        .login-form-container .stButton>button:hover {
            background-image: linear-gradient(to right, #2c3e50 0%, #34495e 100%);
        }
        .login-header-text {
            text-align: center;
            color: #1a5276;
            font-weight: 600;
            font-size: 1.8em;
            margin-bottom: 25px;
        }
        .login-logo { /* MODIFIED */
            display: block;
            margin-left: auto;
            margin-right: auto;
            max-width: 35px; /* Reduced size */
            margin-bottom: 15px;
            /* border-radius: 50%; REMOVED for no oval shape */
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        /* --- Sidebar Styling --- */
        .css-1d391kg {
            background-color: #ffffff;
            padding: 15px !important;
        }
        .sidebar .stButton>button {
             background-image: linear-gradient(to right, #e74c3c 0%, #c0392b 100%);
        }
        .sidebar .stButton>button:hover {
             background-image: linear-gradient(to right, #c0392b 0%, #e74c3c 100%);
        }
        .sidebar .stMarkdown > div > p > strong {
            color: #2c3e50;
        }

        /* --- Option Menu Customization --- */
        div[data-testid="stOptionMenu"] > ul {
            background-color: #ffffff;
            border-radius: 25px;
            padding: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        div[data-testid="stOptionMenu"] > ul > li > button {
            border-radius: 20px;
            margin: 0 5px !important;
            border: none !important;
            transition: all 0.3s ease;
        }
        div[data-testid="stOptionMenu"] > ul > li > button.selected {
            background-image: linear-gradient(to right, #1abc9c 0%, #16a085 100%);
            color: white;
            font-weight: bold;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        div[data-testid="stOptionMenu"] > ul > li > button:hover:not(.selected) {
            background-color: #e0e0e0;
            color: #333;
        }

        /* --- Links --- */
        a {
            color: #3498db;
            text-decoration: none;
            font-weight: 500;
        }
        a:hover {
            text-decoration: underline;
            color: #2980b9;
        }

        /* --- Info/Warning/Error Boxes --- */
        .stAlert {
            border-radius: 8px;
            padding: 15px;
            border-left-width: 5px;
        }
        .stAlert[data-baseweb="notification"][role="alert"] > div:nth-child(2) {
             font-size: 1.0em;
        }
        .stAlert[data-testid="stNotification"] {
            box-shadow: 0 2px 10px rgba(0,0,0,0.07);
        }
        .stAlert[data-baseweb="notification"][kind="info"] { border-left-color: #3498db; }
        .stAlert[data-baseweb="notification"][kind="success"] { border-left-color: #2ecc71; }
        .stAlert[data-baseweb="notification"][kind="warning"] { border-left-color: #f39c12; }
        .stAlert[data-baseweb="notification"][kind="error"] { border-left-color: #e74c3c; }

    </style>
    """, unsafe_allow_html=True)