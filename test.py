import os
import time

# Define the folder path and file name
folder_path = 'my_folder'
file_name = 'my_file.txt'
file_path = os.path.join(folder_path, file_name)
file_content = 'This is some content in the file.'

# Create the folder if it doesn't exist
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Create the file with content
with open(file_path, 'w') as file:
    file.write(file_content)

print(f"File '{file_name}' created with content in '{folder_path}'.")

# Wait for 1 minute (60 seconds)
time.sleep(60)

# Delete the file
if os.path.exists(file_path):
    os.remove(file_path)
    print(f"File '{file_name}' has been deleted.")
else:
    print(f"File '{file_name}' does not exist.")
