# lib for uosql database connection
require 'socket'

GREET = 0
LOGIN = 1
COMMAND = 2

PkgType = Hash.new()
PkgType[GREET] = [0,0,0,0]
PkgType[LOGIN] = [0,0,0,1]
PkgType[COMMAND] = [0,0,0,2]

class Login 
# """ Doc comments """
    attr_reader :username
    attr_reader :password

    def initialize (usern, passw)
        @username = usern
        @password = passw
    end

    def get_username
        @username
    end
end

class Greeting
    def initialize(protocol_ver, msg)
        @protocol_version = protocol_ver
        @message = msg
    end
end

class ClientErrMsg

    def initialize(codenr, message)
        @code = codenr
        @msg = message
    end
end
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

def connect (addr, portnr, username, password)
    """ static method to get a working instance of Connection"""
    begin
        Connection.new(addr, portnr, username, password)
    rescue Exception
        nil
    end
end

class Connection

    def initialize(addr, portnr, username, password)
        @login = Login.new(username, password)
        @ip_addr = addr
        @port = portnr
        connect()
    end

    private
    def connect()
        @tcp = TCPSocket.new @ip_addr, @port
        puts "receive_greeting"
        receive_greeting
        puts "send login"
        send_login
    end


    private
    def send_login
        """ Send login package with username and password """
        #("puts "send package login [0,0,0,1]"
        begin
            count = 0
            # puts "theoretically [0,0,0,1] is sent"
            @tcp.send([LOGIN].pack("N"),0)
            count += 4
            @tcp.send([@login.username.length].pack("Q>"), 0)
            count += 8

            # @tcp.write(@login.username)
            @tcp.send(@login.username,0)
            count += @login.username.length
            @tcp.send([@login.password.length].pack("Q>"),0)
            count += 8


            #@tcp.write(@login.password)
            @tcp.send(@login.password,0)
            count += @login.password.length
            puts count
            
        rescue => err
            puts "error: ", err
            raise
        end

    end

    private
    def receive_greeting
        """Receive the greeting package with version number and a
            message from the server """
        begin
            
            pkg = @tcp.recv(4)
            puts "pkg type"
            arr = pkg.bytes.to_a # array of bytes : pkg type
            if arr != PkgType[GREET]
                puts "unknown pkg, close connection, return nil"
                @tcp.close()
                return nil
            end

            vrs_nr = @tcp.recv(1)
            
            pkg = @tcp.recv(8)
            num = byte_array_to_int(pkg.bytes.to_a)
            
            puts "msg itself"
            msg = @tcp.recv(num)
            puts msg
            
            @greet = Greeting.new vrs_nr, msg
        rescue => e
            puts e
            raise 
        end


    end

    private 
    def int_to_byte_array(length)
        """ Convert an integer value to an eight byte array """
        array = [0,0,0,0,0,0,0,0] # default values
        for i in 0..7
            array[i] = length / 256**(7-i)
            length = length -array[i] * 256**(7-i)
        end
        array
    end

    private
    def byte_array_to_int(length_array)
        """ Converts a four byte array to an integer value """
        len = 0
        for i in 0..7
            len += (length_array[7-i] * (256**i))
        end
        len
    end

    public
    
=begin
        def ping()
        def quit()
        # @tcp.
        end
        def execute()
        def get_version()
        def get_address()
        def get_port()
        def get_username()
        def get_greeting_msg()
=end
end
def int_to_byte_array(length)
    """ Convert an integer value to an eight byte array """
    array = [0,0,0,0,0,0,0,0] # default values
    for i in 0..7
        array[i] = length / 256**(7-i)
        length = length -array[i] * 256**(7-i)
    end
    array
end
##################### Tests ##########################################

=begin
=end
conn = connect("127.0.0.1", 4242, "con", "nect")
if conn.class == Connection
    puts "connection established"
else
    puts "connection failed"
end


