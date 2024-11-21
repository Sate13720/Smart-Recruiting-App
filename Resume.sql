CREATE TABLE Resumes (
    resume_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    resume_data TEXT,
    parsed_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);
