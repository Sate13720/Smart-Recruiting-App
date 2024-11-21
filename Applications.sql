CREATE TABLE Applications (
    application_id INT PRIMARY KEY AUTO_INCREMENT,
    job_id INT,
    user_id INT,
    resume_id INT,
    status ENUM('applied', 'reviewed', 'interview_scheduled', 'offered', 'rejected') DEFAULT 'applied',
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES Jobs(job_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (resume_id) REFERENCES Resumes(resume_id)
);
