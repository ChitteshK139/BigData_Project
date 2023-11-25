from flask import Flask, jsonify, request

app = Flask(__name__)

request_count = 0
response_count = 0

@app.route('/')
def index():
    return jsonify({'message': 'Hello, this is the root endpoint!'})

@app.route('/ping')
def ping():
    global request_count
    global response_count
    
    request_count += 1
    response_count += 1
    return jsonify({'message': 'pong'})

@app.route('/metrics')
def metrics():
    global request_count
    global response_count
    return jsonify({'requests': request_count, 'responses': response_count})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000,debug=True)