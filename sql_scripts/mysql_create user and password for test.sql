/*
 you must logged in as root
 */
CREATE USER 'user_name'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON * . * TO 'user_name'@'%';
FLUSH PRIVILEGES;