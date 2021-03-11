import socket

host = '127.0.0.1'
port = 5001
server = socket.socket()
server.bind((host,port))
server.listen()
conn, addr = server.accept()
print ("Connection from: " + str(addr))
while True:
    data = conn.recv(1024).decode()
    if not data:
        break
    print ("from connected  user: " + str(data))
    data = str(data).upper()
    print ("Received from User: " + str(data))
    data = input("type message: ")
    conn.send(data.encode())
conn.close()