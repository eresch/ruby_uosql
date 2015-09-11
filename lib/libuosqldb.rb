# lib for uosql database connection

# todo class for connection

class Login 
# """ Doc comments """
    def initialize (usern, passw)
        @username = usern
        @password = passw
    end

    def get_username
        @username
    end
end


login = Login.new("library", "uosql")
puts login.get_username


class Connection
=begin
    variables:
        - Greeting
        - ip address string
        - port
        - Login

=end
end
=begin
    def connect(ip_add = "0.0.0.0", )

    def ping()

    def quit()

    def execute()

    def get_version()

    def get_address()

    def get_port()

    def get_username()

    def get_greeting_msg()


end
=end
=begin
# Define Greeting
class Greeting
    def initialize(protocol_ver, msg)
        @protocol_version = protocol_ver
        @message = msg
    end
end
=end


=begin
class ClientErrMsg

end
=end
=begin
class PkgType 

    [0,0,0,0] = Greet
    [0,0,0,1] = Login,
    [0,0,0,2] = Command,
    [0,0,0,3] = Error,
    [0,0,0,4] = Ok,
    [0,0,0,5] = Response,
    [0,0,0,6] = AccDenied,
    [0,0,0,7] = AccGranted

end
=end