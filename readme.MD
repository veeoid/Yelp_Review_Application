# 📍 Yelp Review Application

A Python-based desktop application that emulates core functionalities of Yelp, allowing users to explore businesses and their reviews through a user-friendly interface. This project leverages Apache Cassandra for scalable data storage and Tkinter for the graphical user interface.

---

## 🧰 Tech Stack

- **Backend**: Python, Apache Cassandra
- **Frontend**: Tkinter (Python's standard GUI library)
- **Data Source**: [Yelp Open Dataset](https://www.yelp.com/dataset)

---

## 📁 Project Structure

Yelp_Review_Application/ 
├── creation_schema.py # Defines Cassandra keyspace and tables 
├── load_data.py # Loads Yelp dataset into Cassandra 
├── yelp_app.py # Launches the Tkinter GUI application 
├── yelp-logo-22.png # Yelp logo used in the GUI 
└── README.md # Project documentation


---

## ⚙️ Setup Instructions

### 1. Prerequisites

- **Python 3.x** installed
- **Apache Cassandra** installed and running
- Required Python libraries:
  - `cassandra-driver`
  - `tkinter` (usually included with Python)

Install the Cassandra driver using pip:
```
pip install cassandra-driver
```

###2. Download and Extract Yelp Dataset
- Download the dataset from the Yelp Open Dataset.
- Extract the contents to a known directory on your system.

###3. Create Cassandra Keyspace and Tables
- Run the `creation_schema.py` script to set up the necessary keyspace and tables in Cassandra:
```
python creation_schema.py
```
###4. Load Data into Cassandra
Use the `load_data.py` script to parse the Yelp dataset and insert the data into Cassandra:
```
python load_data.py
```
Note: Ensure that the dataset files are correctly referenced in the script.
###5. Launch the Application
Start the Tkinter GUI application using:
```
python yelp_app.py
```

🖼️ Application Features
Business Search: Find businesses by name or category.

Review Browsing: View reviews associated with selected businesses.

User-Friendly Interface: Intuitive GUI for seamless navigation.

📌 Notes
Ensure that Apache Cassandra is running before executing the scripts.

Modify file paths in the scripts if your dataset is located in a different directory.


