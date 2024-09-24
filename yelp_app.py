# __author__ = "Vismay Chaudhari"
"""
This code is a sample application that uses the Yelp dataset to display businesses and their reviews.
"""

import tkinter as tk
from tkinter import messagebox, simpledialog
from cassandra.cluster import Cluster
from uuid import uuid4
import datetime
from PIL import Image, ImageTk

# Establish a connection to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('yelp_data')

# Define styles
LARGE_FONT = ("Verdana", 12)
SMALL_FONT = ("Verdana", 10)
BUTTON_STYLE = {"font": SMALL_FONT, "bg": "#E1E1E1", "padx": 5, "pady": 5}
LOGO_PATH = "yelp-logo-22.png"  # Update this path

# Main application window
class Businesses(tk.Tk):
    """
    Window that displays a list of businesses
    and its reviews when clicked.
    """
    def __init__(self):
        super().__init__()
        self.title('Businesses') # Set the window title
        self.geometry('1920x1080') # Set the window size and position
        self.configure(bg='#143d59') # Set the window background color

        # Logo
        self.original_logo = Image.open('yelp-logo-22.png')   # Open the logo image
        self.resized_logo = self.original_logo.resize((200, 80), Image.Resampling.LANCZOS)  # Set the desired size
        self.logo_image = ImageTk.PhotoImage(self.resized_logo)
        self.logo_label = tk.Label(self, image=self.logo_image, bg='#f4b41a') # Create a label with the logo image
        self.logo_label.pack(side='left', anchor='nw', padx=10, pady=10) # Place the label in the window

        # Search bar
        self.search_var = tk.StringVar() # Create a variable to store the search text
        self.search_entry = tk.Entry(self, textvariable=self.search_var, font=SMALL_FONT, width=50) # Create a text entry field
        self.search_entry.pack(side='top', padx=10, pady=10) # Place the entry field in the window
        self.search_button = tk.Button(self, text='Search', command=self.refresh_all_businesses, **BUTTON_STYLE) # Create a button to search for businesse
        self.search_button.pack(side='top', padx=10, pady=10) # Place the button in the window
        self.search_var.set("Search for businesses") # Set the default text in the search field
        self.search_entry.bind("<FocusIn>", lambda args: self.search_entry.delete('0', 'end')) # Clear the search field when clicked
        self.search_entry.bind("<FocusOut>", lambda args: self.search_var.set("Search for businesses")) # Restore the default text when the search field loses focus


        # Business list frame (with transparent background)
        self.business_frame = tk.Frame(self, bg='#f4b41a', bd=5)
        self.business_frame.pack(fill='both', expand=True, padx=10, pady=10)
        self.refresh_all_businesses()

    def refresh_all_businesses(self):
        """
        Refresh the list of businesses in the UI
        :return:
        """
        print("Refreshing businesses")
        for widget in self.business_frame.winfo_children():
            widget.destroy()
        for business in self.fetch_businesses_from_db():
            self.display_business(business)

    def fetch_businesses_from_db(self):
        """
        Fetch the list of businesses from the database
        :return:
        """
        limit = 50
        business_rows = session.execute('SELECT business_id, name, address, city, stars, review_count FROM business LIMIT %s', (limit,))
        # print(business_rows)
        return business_rows

    def display_business(self, business):
        """
        Display a business in the UI
        :param business:
        :return:
        """
        business_frame = tk.Frame(self.business_frame, borderwidth=2, relief='sunken')
        business_frame.pack(side='top', fill='x', padx=10, pady=5)
        tk.Label(business_frame, text=business.name, font=("Script", 25)).pack(side='left')
        tk.Label(business_frame, text=business.address+', '+business.city).pack(side='left', padx=10)
        tk.Label(business_frame, text='Ratings: '+str(business.stars)).pack(side='right', padx=10)
        tk.Label(business_frame, text='Reviews: '+str(business.review_count)).pack(side='right', padx=10)
        review_button = tk.Button(business_frame, text='Reviews', command=lambda b=business: self.open_review_window(b))
        review_button.pack(side='right', padx=10)

    def open_review_window(self, business):
        """
        Open the review window for a business
        :param business:
        :return:
        """
        print("Opening review window for business: " + business.name)
        new_window = FoodReviewApp(business)
        new_window.grab_set()
        new_window.wait_window()
        # self.refresh_businesses()

class FoodReviewApp(tk.Toplevel):
    """
    Window that displays the reviews for a business
    """
    def __init__(self, business):
        """
        Initialize the window
        :param business:
        """
        print("Initializing FoodReviewApp")
        super().__init__()  # Remove the master parameter
        self.business = business
        self.initialize_ui()


    def initialize_ui(self):
        """
        Initialize the UI
        :return:
        """
        self.title('Food Reviews for ' + self.business.name)
        self.geometry('800x600')
        self.title('Food Reviews for ' + self.business.name)
        self.geometry('800x600')
        add_review_button = tk.Button(self, text='Add Review', command=self.review_add_button)
        add_review_button.pack(side='top', padx=10, pady=5)
        self.reviews_frame = tk.Frame(self, borderwidth=2, relief='sunken')
        self.reviews_frame.pack(fill='both', expand=True)
        self.refresh_all_reviews()

    def fetch_reviews_from_db(self):
        """
        Fetch the reviews for the business from the database
        :return:
        """
        limit = 10000000
        # Fetch the reviews without ordering
        review_rows = session.execute(
            'SELECT review_id, text, user_id, date FROM review WHERE business_id = %s LIMIT %s',
            (self.business.business_id, limit)
        )
        # Convert to list for sorting
        reviews = list(review_rows)
        # Sort the reviews in descending order by date in your application
        reviews.sort(key=lambda x: x.date, reverse=True)
        # Generate the list of reviews with user names
        row_list = []
        for review_row in reviews:
            user_row = session.execute('SELECT name FROM user WHERE user_id = %s', (review_row.user_id,))
            user_name = user_row.one().name if user_row.one() else 'Unknown'
            row_list.append({
                'id': review_row.review_id,
                'review': review_row.text,
                'user': user_name,
                'date': review_row.date  # Include the review date in the dictionary
            })
            if len(row_list) == limit:
                break
        return row_list

    def refresh_all_reviews(self):
        print("Refreshing reviews")
        for widget in self.reviews_frame.winfo_children():
            widget.destroy()

        try:
            reviews = self.fetch_reviews_from_db()
            print(f"Number of reviews fetched: {len(reviews)}")
            for review in reviews:
                self.show_all_reviews(review)
        except Exception as e:
            print(f"An error occurred while fetching reviews: {e}")

    def show_all_reviews(self, review):
        review_frame = tk.Frame(self.reviews_frame, borderwidth=2, relief='sunken')
        review_frame.pack(side='top', fill='x', padx=10, pady=5)
        user_id_label = tk.Label(review_frame, text=review['user'])
        user_id_label.pack(side='left', fill='x', expand=True)
        tk.Label(review_frame, text=review['review'][:40]).pack(side='left', fill='x', expand=True)
        edit_button = tk.Button(review_frame, text='Edit', command=lambda r=review: self.review_edit_button(r))
        edit_button.pack(side='right', padx=5)
        show_full = tk.Button(review_frame, text='Show Full', command=lambda r=review: messagebox.showinfo("Review", r['review']))
        show_full.pack(side='right', padx=5)
        delete_button = tk.Button(review_frame, text='Delete', command=lambda r=review: self.review_delete_button(r))
        delete_button.pack(side='right', padx=5)

    def review_add_button(self):
        """
        Add a new review for the business
        :return:
        """
        review_text = simpledialog.askstring("New Review", "Enter your review:")
        user_name = simpledialog.askstring("Your Name", "Enter your name:")

        if review_text and user_name:
            user_id = str(uuid4())
            # Insert the new user into the user table.
            session.execute(
                'INSERT INTO user (user_id, name) VALUES (%s, %s)',
                (user_id, user_name)
            )
            # Get the current datetime in the required format
            current_time = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            # Insert the new review with the current timestamp.
            review_id = str(uuid4())
            print(review_id)
            session.execute(
                'INSERT INTO review (review_id, text, user_id, date, business_id) VALUES (%s, %s, %s, %s, %s)',
                (review_id, review_text, user_id, current_time, self.business.business_id)
            )
            self.refresh_all_reviews()

    def review_delete_button(self, review):
        """
        Delete a review
        :param review:
        :return:
        """
        if messagebox.askyesno("Delete Review", "Are you sure you want to delete this review?"):
            session.execute('DELETE FROM review WHERE review_id = %s', (review['id'],))
            self.refresh_all_reviews()

    def review_edit_button(self, review):
        """
        Edit a review
        :param review:
        :return:
        """
        new_review_text = simpledialog.askstring("Edit Review", "Enter your new review:", initialvalue=review['review'])
        if new_review_text and new_review_text != review['review']:
            session.execute(
                "UPDATE review SET text = %s WHERE review_id = %s",
                (new_review_text, review['id'])
            )
            self.refresh_all_reviews()


if __name__ == '__main__':
    try:
        business = Businesses()
        business.mainloop()
    finally:
        cluster.shutdown()