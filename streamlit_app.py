import streamlit as st
import psycopg2
import re

# Function to apply a custom style
def set_custom_style():
    # Define your custom style here
    custom_style = """
    <style>
    /* Change the background color of the main page */
    .stApp {
        background-color: #0E1117;
    }

    /* Change the text color of the title to white */
    h1 {
        color: white;
    }

    /* Change the label color to white */
    .stTextInput label {
        color: white !important;
    }

    /* Change the text color inside the text input box to black */
    .stTextInput input {
        color: black;
    }

    /* Change the placeholder color inside the text input box to a lighter shade */
    .stTextInput input::placeholder {
        color: #AAAAAA;
    }

    /* This is to ensure that warning and error messages are readable */
    .stAlert {
        background-color: #0E1117;
        color: white;
    }

    /* Other style changes you want to make */
    </style>
    """

    # Apply the custom style
    st.markdown(custom_style, unsafe_allow_html=True)

# Apply the custom style
set_custom_style()

# Function to extract video ID from YouTube URL
def extract_video_id(url):
    regex = r"(https?://)?(www\.)?(youtube\.com|youtu\.?be)/.+$"
    if re.match(regex, url):
        video_id = url.split("v=")[-1].split("&")[0]
        return video_id
    return None


# Function to check if video ID exists in the database
def check_video_id_exists(conn, video_id):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM akool_youtube_ids WHERE id = %s", (video_id,))
        return cur.fetchone() is not None


# Function to add video ID to the database
def add_video_id(conn, video_id):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO akool_youtube_ids (id) VALUES (%s)", (video_id,))
        conn.commit()

# Streamlit app
def main():
    #st.image("Images/SaharaAI_logo.png", width=250)

    st.title("SaharaAI YouTube video collector ")

    # Database connection
    conn = psycopg2.connect(
        host="ep-calm-star-76049617.us-west-2.retooldb.com",
        database="retool",
        user="retool",
        password="hed3ZnNfxy6B"
    )

    # Input for YouTube URL
    youtube_url = st.text_input("Enter YouTube URL:")

    # Send button
    if st.button("Send"):
        if youtube_url:
            video_id = extract_video_id(youtube_url)
            if video_id:
                if check_video_id_exists(conn, video_id):
                    st.warning("Error! This video has already been added, please add another one!")
                else:
                    add_video_id(conn, video_id)
                    st.success("Success! Video has been sent!")
            else:
                st.error("Invalid YouTube URL.")
        else:
            st.error("Please enter a YouTube URL.")

    conn.close()


if __name__ == "__main__":
    main()
