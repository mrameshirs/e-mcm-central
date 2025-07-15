# ui_login.py
import streamlit as st
import os
import base64
from config import USER_CREDENTIALS, USER_ROLES, AUDIT_GROUP_NUMBERS

def login_page():
    #st.markdown("<div class='page-main-title'>e-MCM App</div>", unsafe_allow_html=True)
    # Define the CSS style
    st.markdown("""
    <style>
    .page-main-title {
        font-size: 3rem; /* Adjust the font size as needed */
        font-weight: bold;
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Render the title
    st.markdown("<div class='page-main-title'>e-MCM App</div>", unsafe_allow_html=True)
    st.markdown("<h2 class='page-app-subtitle'>GST Audit 1 Commissionerate</h2>", unsafe_allow_html=True)

    def get_image_base64_str(img_path):
        try:
            with open(img_path, "rb") as img_file:
                return base64.b64encode(img_file.read()).decode('utf-8')
        except FileNotFoundError:
            st.error(f"Logo image not found at path: {img_path}. Ensure 'logo.png' is present.")
            return None
        except Exception as e:
            st.error(f"Error reading image file {img_path}: {e}")
            return None

    image_path = "logo.png" # Ensure logo.png is in the same directory
    base64_image = get_image_base64_str(image_path)
    if base64_image:
        image_type = os.path.splitext(image_path)[1].lower().replace(".", "") or "png"
        st.markdown(
            f"<div class='login-header'><img src='data:image/{image_type};base64,{base64_image}' alt='Logo' class='login-logo'></div>",
            unsafe_allow_html=True)
    else:
        st.markdown("<div class='login-header' style='color: red; font-weight: bold;'>[Logo Not Found]</div>",
                    unsafe_allow_html=True)

    #st.markdown("<h5 class='login-header-text'>User Login</h5>", unsafe_allow_html=True)
    st.markdown("""
    <div class='app-description'>
        Welcome! This digital platform streamlines Draft Audit Report (DAR) collection , processing and compilation from Audit Groups for MCM 
         purpose using  AI-powered data extraction.
    </div>
    """, unsafe_allow_html=True)

    # The login form elements should be within a container if you want the white box effect.
    # If .login-form-container CSS is defined globally, this might not be strictly necessary
    # but for clarity, you might wrap form elements:
    # with st.container(): # or st.form for a clear submit action
    #    st.markdown("<div class='login-form-container'>", unsafe_allow_html=True) # If you want the CSS box
    username = st.text_input("Username", key="login_username_styled", placeholder="Enter your username")
    password = st.text_input("Password", type="password", key="login_password_styled",
                             placeholder="Enter your password")

    if st.button("Login", key="login_button_styled", use_container_width=True):
        if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
            st.session_state.logged_in = True
            st.session_state.username = username
            st.session_state.role = USER_ROLES[username]
            if st.session_state.role == "AuditGroup":
                st.session_state.audit_group_no = AUDIT_GROUP_NUMBERS[username]
            st.success(f"Logged in as {username} ({st.session_state.role})")
            st.session_state.drive_structure_initialized = False
            st.rerun()
        else:
            st.error("Invalid username or password")
    #    st.markdown("</div>", unsafe_allow_html=True) # Close CSS box if opened
