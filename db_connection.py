#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
import string

def connectDataBase():

    # Create a database connection object using psycopg2
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            dbname="assignmentdb",
            user="postgres",
            password="koko02",
            host="localhost",
            port="5432"
        )
        return conn
        print("Connected to db")
    except:
        print("Error: Unable to connect to the database")
        return None, None

def createCategory(cur, catId, catName):

    # Insert a category in the database
    try:
        cur.execute("INSERT INTO categories (id, name) VALUES (%s, %s)", (catId, catName))
    except Exception:
        print("Error: Issue creating category")

def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    try:
        # 1 Get the category id based on the informed category name
        cur.execute("SELECT id FROM categories WHERE name=%s", (docCat,))
        cat_id = cur.fetchone()[0]

        # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
        cur.execute("INSERT INTO documents (id, text, title, date, category_id, num_chars) VALUES (%s, %s, %s, %s, %s, %s)",
        (docId, docText, docTitle, docDate, cat_id, len(docText.translate(str.maketrans('', '', string.punctuation)).replace(' ', ''))))

        # 3 Update the potential new terms.
        # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
        # 3.2 For each term identified, check if the term already exists in the database
        # 3.3 In case the term does not exist, insert it into the database
        terms = docText.translate(str.maketrans('', '', string.punctuation)).lower().split()
        for term in terms:
            cur.execute("SELECT EXISTS(SELECT 1 FROM terms WHERE term=%s)", (term,))
            if not cur.fetchone()[0]:
                cur.execute("INSERT INTO terms (term) VALUES (%s)", (term,))

        # 4 Update the index
        # 4.1 Find all terms that belong to the document
        # 4.2 Create a data structure the stores how many times (count) each term appears in the document
        # 4.3 Insert the term and its corresponding count into the database
        term_counts = {term: terms.count(term) for term in set(terms)}
        for term, count in term_counts.items():
            cur.execute("INSERT INTO doc_index (term, doc_id, count) VALUES (%s, %s, %s)", (term, docId, count))
    except Exception:
        print("Error: Issue creating document")

def deleteDocument(cur, docId):
    try:
        # 1 Query the index based on the document to identify terms
        # 1.1 For each term identified, delete its occurrences in the index for that document
        # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
        cur.execute("SELECT term FROM doc_index WHERE doc_id=%s", (docId,))
        terms = cur.fetchall()

        for term in terms:
            cur.execute("DELETE FROM doc_index WHERE term=%s AND doc_id=%s", (term, docId))
            cur.execute("SELECT EXISTS(SELECT 1 FROM doc_index WHERE term=%s)", (term,))
            if not cur.fetchone()[0]:
                cur.execute("DELETE FROM terms WHERE term=%s", (term,))

        # 2 Delete the document from the database
        cur.execute("DELETE FROM documents WHERE id=%s", (docId,))
    except Exception:
        print("Error: Issue deleting document")

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    try:
        # 1 Delete the document
        deleteDocument(cur, docId)

        # 2 Create the document with the same id
        createDocument(cur, docId, docText, docTitle, docDate, docCat)
    except Exception:
        print("Error: Issue updating document")
        
        
def modifyDocument(cur, doc_id, text, title, date, category):
    try:
        # First, delete old terms from the index
        cur.execute("DELETE FROM doc_index WHERE doc_id=?", (doc_id,))
        # Update the document details
        cur.execute("UPDATE documents SET text=?, title=?, date=?, category=? WHERE id=?", 
                    (text, title, date, category, doc_id))
        # Now, re-index the updated document
        terms = text.split()
        term_freq = {term: terms.count(term) for term in set(terms)}
        for term, freq in term_freq.items():
            cur.execute("INSERT INTO doc_index (doc_id, term, count) VALUES (?, ?, ?)", (doc_id, term, freq))
    except Exception:
        print("Error: Issue modifying document")


def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    try:
        cur.execute("""
            SELECT term, documents.title, count 
            FROM doc_index
            JOIN documents ON doc_index.doc_id = documents.id
            ORDER BY term, documents.title
        """)
        rows = cur.fetchall()
        index = {}
        for row in rows:
            term, title, count = row
            key = f"{title}:{count}"
            if term in index:
                index[term] += "," + key
            else:
                index[term] = key
        return index
    except Exception:
        print("Error: Issue getting doc_index")