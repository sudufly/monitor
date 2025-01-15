#!/usr/bin/python
# coding:utf-8
import SocketServer
import BaseHTTPServer
import CGIHTTPServer

class ReusableTCPServer(SocketServer.TCPServer):
    allow_reuse_address = True  # 允许端口复用

class SimplePostHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        # 获取请求头中的内容长度
        content_length = int(self.headers.getheader('content-length', 0))
        # 读取请求体
        post_data = self.rfile.read(content_length)

        # 解析 POST 数据（这里假设是表单数据或JSON）
        response_content = "Received POST request.\n"
        #        if 'application/json' in self.headers.getheader('content-type', ''):
        #            import json
        #            data = json.loads(post_data)
        #            response_content += "Parsed JSON: {}\n".format(json.dumps(data, indent=4))
        #        else:
        response_content += "Raw POST Data: {}\n".format(post_data)

        # 设置响应头
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()

        # 发送响应体
        self.wfile.write(response_content)

    def do_GET(self):
        # 简单的 GET 请求处理
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write("Hello, this is a simple GET response.")

def run(server_class=ReusableTCPServer, handler_class=SimplePostHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print "Starting httpd server on port {port}...".format(port=port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print "\nShutting down server..."
        httpd.server_close()

if __name__ == "__main__":
    run()
