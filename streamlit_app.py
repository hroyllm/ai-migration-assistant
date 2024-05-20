import streamlit as st

# Main menu items
main_menu = ["Home", "Settings", "About"]

# Render the main menu in the sidebar
selected_main_menu = st.sidebar.radio("Main Menu", main_menu)

# Show different sub-menus based on the main menu selection
if selected_main_menu == "Home":
    st.sidebar.subheader("Home Sub-Menu")
    home_sub_menu = st.sidebar.radio("Select an option", ["Option 1", "Option 2", "Option 3"])

    if home_sub_menu == "Option 1":
        st.write("Home -> Option 1 selected")
    elif home_sub_menu == "Option 2":
        st.write("Home -> Option 2 selected")
    elif home_sub_menu == "Option 3":
        st.write("Home -> Option 3 selected")

elif selected_main_menu == "Settings":
    st.sidebar.subheader("Settings Sub-Menu")
    settings_sub_menu = st.sidebar.radio("Select an option", ["Profile", "Privacy", "Notifications"])

    if settings_sub_menu == "Profile":
        st.write("Settings -> Profile selected")
    elif settings_sub_menu == "Privacy":
        st.write("Settings -> Privacy selected")
    elif settings_sub_menu == "Notifications":
        st.write("Settings -> Notifications selected")

elif selected_main_menu == "About":
    st.sidebar.subheader("About Sub-Menu")
    about_sub_menu = st.sidebar.radio("Select an option", ["Company", "Team", "Contact"])

    if about_sub_menu == "Company":
        st.write("About -> Company selected")
    elif about_sub_menu == "Team":
        st.write("About -> Team selected")
    elif about_sub_menu == "Contact":
        st.write("About -> Contact selected")

# Main content based on main menu selection
if selected_main_menu == "Home":
    st.title("Home")
    st.write("Welcome to the Home page!")
elif selected_main_menu == "Settings":
    st.title("Settings")
    st.write("Adjust your settings here.")
elif selected_main_menu == "About":
    st.title("About")
    st.write("Learn more about us.")
