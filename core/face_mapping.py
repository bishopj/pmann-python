import face_recognition

# Load your image
image = face_recognition.load_image_file("family_photo.jpg")

# Find all face locations
face_locations = face_recognition.face_locations(image)

# Optionally get facial landmarks
face_landmarks = face_recognition.face_landmarks(image)

print(f"Found {len(face_locations)} face(s)")
