CREATE TABLE Users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    role ENUM('recruiter', 'candidate') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
