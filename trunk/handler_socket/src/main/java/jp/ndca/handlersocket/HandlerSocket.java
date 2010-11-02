package jp.ndca.handlersocket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * MySQL pluginの一つであるHandlerSocket(http://github.com/ahiguti/HandlerSocket-Plugin-for-MySQL/)のJavaクライアント実装です。
 * 
 * @author moaikids
 *
 */
public class HandlerSocket {
	private static final Log log = LogFactory.getLog(HandlerSocket.class);
	private static final byte TOKEN_SEPARATOR = 0x09;
	private static final byte COMMAND_TERMINATE = 0x0a;

	private static final int TIMEOUT = 30 * 1000;
	private static final int SOCKET_BUFFER_SIZE = 8 * 1024;
	private static final int EXECUTE_BUFFER_SIZE = 8 * 1024;
	
	private int readTimeout = TIMEOUT;
	private int timeout = TIMEOUT;
	private int sendBufferSize = SOCKET_BUFFER_SIZE;
	private int receiveBufferSize = SOCKET_BUFFER_SIZE;
	private int executeBufferSize = EXECUTE_BUFFER_SIZE;

	Socket socket;
	BlockingQueue<byte[]> commandBuffer;
	Command command;
	int currentResultSize = 0;//直前に実行されたコマンドのレスポンスデータサイズ
	
	public HandlerSocket(){
		super();
		commandBuffer = new LinkedBlockingQueue<byte[]>();
		command = new Command();
	}
	
	public Command command(){
		return command;
	}
	
	/**
	 * 現在未発行のコマンドの総バイトサイズを返却します。
	 * @return
	 */
	public int getCommandSize(){
		int total = 0;
		for(byte[] b : commandBuffer){
			total += b.length;
		}
		
		return total;
	}
	
	/**
	 * 直前に実行されたコマンドのレスポンスデータサイズを返却します。
	 * @return
	 */
	public int getCurrentResponseSize(){
		return currentResultSize;
	}
	
	/**
	 * HandlerSocketと接続します。
	 * @param host
	 * @param port
	 * @throws IOException
	 */
	public void open(String host, int port) throws IOException{
		open(InetAddress.getByName(host), port);
	}
	
	/**
	 * HandlerSocketと接続します。
	 * @param address
	 * @param port
	 * @throws IOException
	 */
	public void open(InetAddress address, int port) throws IOException{
		if(socket != null && socket.isConnected()){
			close();
		}
		
		socket = new Socket();
		socket.setSendBufferSize(sendBufferSize);
		socket.setReceiveBufferSize(receiveBufferSize);
		socket.setSoTimeout(timeout);
		socket.setTcpNoDelay(true);
		
		System.out.println(socket.getSendBufferSize() + "/" + socket.getReceiveBufferSize());
		
		socket.connect(new InetSocketAddress(address, port), readTimeout);
	}
	
	public List<HandlerSocketResult> execute() throws IOException{
		//TODO コマンドが一つもない場合の処理はどうするか？今回は何もしないでnullを返す。
		if(commandBuffer.size() == 0)
			return null;
		currentResultSize = 0;

		List<HandlerSocketResult> results = new ArrayList<HandlerSocketResult>();
		//TODO HandlerSocketとの送受信をすべてインラインで記述するか？ひとまずインラインで。
		//TODO OutputStream数珠つなぎの影響で無駄なbufferコピーが発生してないか。調べて最適な形に。
		//TODO 送受信途中でエラーが発生した場合どうすれば良いか。フェールセーフな方式の検討。
		//TODO 一度に実行するコマンドの上限を設けるか？今は無制限。
//		DataOutputStream os = null;
		InputStream is = null;
		try{
			final ByteArrayOutputStream commands = new ByteArrayOutputStream();
			for(byte[] command = null ; (command = commandBuffer.poll())!= null ; ){
				commands.write(command);
			}

//			os = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), executeBufferSize));
			is = socket.getInputStream();

			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			byte[] b = new byte[executeBufferSize];
			
//			new Thread(new Runnable(){
//				@Override
//				public void run() {
//					try {
						OutputStream os =  socket.getOutputStream();
						os.write(commands.toByteArray());
						os.flush();
//					} catch (IOException e) {
//						log.error(e);
//					}
//				}
				
//			}).start();
			
			int totalSize = 0;
			for(int size = 0 ; (size = is.read(b)) > 0 ; ){
				totalSize += size;
				buffer.write(b, 0, size);
				if(size < executeBufferSize)
					break;
			}
			
			System.out.println(totalSize);
			
			ResponseParser parser = new ResponseParser();
			results = parser.parse(buffer.toByteArray());
		}finally{
			
		}
		
		return results;
	}
	
	/**
	 * HandlerSocketとの接続を切断します。
	 * @throws IOException
	 */
	public void close() throws IOException{
		socket.close();
	}
	
	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getSendBufferSize() {
		return sendBufferSize;
	}

	public void setSendBufferSize(int sendBufferSize) {
		this.sendBufferSize = sendBufferSize;
	}

	public int getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public void setReceiveBufferSize(int receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}

	public int getExecuteBufferSize() {
		return executeBufferSize;
	}

	public void setExecuteBufferSize(int executeBufferSize) {
		this.executeBufferSize = executeBufferSize;
	}



	/**
	 * HanlerSocketのコマンドを実行します。実行したコマンドはqueueに格納され、HanlerSocket#execute時にまとめて実行されます。
	 * @author moaikids
	 *
	 */
	public class Command{
		private static final String OPERATOR_OPEN_INDEX = "P";
		private static final String OPERATOR_INSERT = "+";
		private static final String OPERATOR_UPDATE = "U";
		private static final String OPERATOR_DELETE = "D";
		
		private static final String DEFAULT_ENCODING = "UTF-8";
		private String encoding = DEFAULT_ENCODING;
		
		public Command(){
			super();
		}
		
		public Command(String encoding){
			this();
			this.encoding = encoding;
		}
		
		/**
		 * open_indexコマンドを実行します。
		 * @param id
		 * @param db
		 * @param table
		 * @param index
		 * @param fieldList
		 * @throws IOException
		 */
		public void openIndex(String id, String db, String table, String index, String fieldList) throws IOException{
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(buffer);
			
			//header
			writeToken(dos, OPERATOR_OPEN_INDEX);
			writeTokenSeparator(dos);
			//id
			writeToken(dos, id);
			writeTokenSeparator(dos);
			//db
			writeToken(dos, db);
			writeTokenSeparator(dos);
			//table
			writeToken(dos, table);
			writeTokenSeparator(dos);
			//index
			writeToken(dos, index);
			writeTokenSeparator(dos);
			//field list
			writeToken(dos, fieldList);
			writeCommandTerminate(dos);
			
			dos.flush();
			
			commandBuffer.add(buffer.toByteArray());
		}

		/**
		 * findコマンドを実行します。
		 * @param id
		 * @param keys
		 * @throws IOException
		 */
		public void find(String id, String[] keys) throws IOException{
			find(id, keys , "=" , "1", "0");
		}
		
		/**
		 * findコマンドを実行します。
		 * @param id
		 * @param keys
		 * @param operator
		 * @param limit
		 * @param offset
		 * @throws IOException
		 */
		public void find(String id, String[] keys, String operator , String limit, String offset) throws IOException{
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(buffer);
			
			//id
			writeToken(dos, id);
			writeTokenSeparator(dos);
			//operator
			writeToken(dos, operator);
			writeTokenSeparator(dos);
			//key nums
			writeToken(dos, String.valueOf(keys.length));
			writeTokenSeparator(dos);
			for(String key : keys){
				writeToken(dos, key);
				writeTokenSeparator(dos);
			}
			//limit
			writeToken(dos, limit);
			writeTokenSeparator(dos);
			//offset
			writeToken(dos, offset);
			writeCommandTerminate(dos);
			
			dos.flush();
			
			commandBuffer.add(buffer.toByteArray());
		}
		
		/**
		 * insertコマンドを実行します。
		 * @param id
		 * @param keys
		 * @throws IOException
		 */
		public void insert(String id, String[] keys) throws IOException{
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(buffer);

			//id
			writeToken(dos, id);
			writeTokenSeparator(dos);
			//operator
			writeToken(dos, OPERATOR_INSERT);
			writeTokenSeparator(dos);
			//key nums
			writeToken(dos, String.valueOf(keys.length));
			writeTokenSeparator(dos);
			for(int i = 0 ; i < keys.length ; i++){
				writeToken(dos, keys[i] == null ? null : keys[i].getBytes(encoding));
				if(i == keys.length - 1){
					writeCommandTerminate(dos);
				}else{
					writeTokenSeparator(dos);
				}
			}
			
			dos.flush();
			
			commandBuffer.add(buffer.toByteArray());
		}
		
		/**
		 * find_modify(delete)を実行します。
		 * @param id
		 * @param keys
		 * @param operator
		 * @param limit
		 * @param offset
		 * @throws IOException
		 */
		public void findModifyDelete(String id, String[] keys, String operator , String limit, String offset) throws IOException{
			findModify(id, keys, operator, limit, offset, OPERATOR_DELETE, new String[keys.length]);
		}
		
		/**
		 * find_modify(update)を実行します。
		 * @param id
		 * @param keys
		 * @param operator
		 * @param limit
		 * @param offset
		 * @param values
		 * @throws IOException
		 */
		public void findModifyUpdate(String id, String[] keys, String operator , String limit, String offset, String[] values) throws IOException{
			findModify(id, keys, operator, limit, offset, OPERATOR_UPDATE, values);
		}
		
		/**
		 * 
		 * @param id
		 * @param keys
		 * @param operator
		 * @param limit
		 * @param offset
		 * @param modOperation
		 * @param values
		 * @throws IOException
		 */
		private void findModify(
				String id, String[] keys, String operator , String limit, String offset, 
				String modOperation, String[] values) throws IOException{
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(buffer);

			//id
			writeToken(dos, id);
			writeTokenSeparator(dos);
			//operator
			writeToken(dos, operator);
			writeTokenSeparator(dos);
			//key nums
			writeToken(dos, String.valueOf(keys.length));
			writeTokenSeparator(dos);
			for(String key : keys){
				writeToken(dos, key == null ? null : key.getBytes(encoding));
				writeTokenSeparator(dos);
			}
			//limit
			writeToken(dos, limit);
			writeTokenSeparator(dos);
			//offset
			writeToken(dos, offset);
			writeTokenSeparator(dos);
			//modify operator
			writeToken(dos, modOperation);
			writeTokenSeparator(dos);
			
			//modify values
			for(int i = 0 ; i < values.length ; i++){
				writeToken(dos, values[i] == null ? null : values[i].getBytes(encoding));
				if(i == values.length - 1){
					writeCommandTerminate(dos);
				}else{
					writeTokenSeparator(dos);
				}
			}
			
			dos.flush();
			
			commandBuffer.add(buffer.toByteArray());
		}
		
		private void writeToken(DataOutputStream dos, String token) throws IOException{
			if(token == null){
				dos.writeByte(0x00);
			}else{
				for(char c : token.toCharArray()){
					if(c > 255){
						dos.writeChar(c);
					}else{
						dos.writeByte((byte)c);
						
					}
				}
			}
		}
		
		private void writeToken(DataOutputStream dos, byte[] token) throws IOException{
			if(token == null){
				dos.writeByte(0x00);
			}else{
				for(byte b : token){
					dos.writeByte(b);
				}
			}
		}
		
		private void writeTokenSeparator(DataOutputStream dos) throws IOException{
			dos.writeByte(TOKEN_SEPARATOR);
		}
		
		private void writeCommandTerminate(DataOutputStream dos) throws IOException{
			dos.writeByte(COMMAND_TERMINATE);
		}
	}
	
	class ResponseParser{
		private static final String DEFAULT_ENCODING = "UTF-8";
		private String encoding = DEFAULT_ENCODING;
		
		public ResponseParser(){
			super();
		}
		public ResponseParser(String encoding){
			this();
			this.encoding = encoding;
		}
		
		public List<HandlerSocketResult> parse(byte[] data) throws UnsupportedEncodingException{
			List<HandlerSocketResult> results = new ArrayList<HandlerSocketResult>();
			//TODO 中途半端で終わったレスポンス内容は破棄しています。その実装で良いか確認。
			for(int i = 0 ; i < data.length ; ){
				List<String> messages = new ArrayList<String>();
				ByteArrayOutputStream buf = new ByteArrayOutputStream();

				HandlerSocketResult result = new HandlerSocketResult();
				int status = data[i] - 0x30 ; i++; if(i >= data.length) break;
				if(data[i] != 0x09)
					throw new RuntimeException();//TODO
				i++; if(i >= data.length) break;//0x09
				int fieldNum = data[i] - 0x30 ; i++; if(i >= data.length) break;
				
				if(data[i] == 0x0a){
					result.setStatus(status);
					result.setFieldNum(fieldNum);
					result.setMessages(messages);

					results.add(result);
					i++;//0x09 or 0x0a

					continue;
				}else{
					i++;//0x09 or 0x0a
				}
				
				while(true){
					if(data.length <= i)
						break;
					byte b = data[i];
					i++;
					if(b == COMMAND_TERMINATE){
						messages.add(buf.toString());

						result.setStatus(status);
						result.setFieldNum(fieldNum);
						result.setMessages(messages);
						
						results.add(result);
						break;
					}
					if(b == TOKEN_SEPARATOR){
						messages.add(new String(buf.toByteArray(), encoding));
						buf = new ByteArrayOutputStream();
						continue;
					}
					buf.write(b);
				}
				
			}
			
			return results;
		}
		public String getEncoding() {
			return encoding;
		}
		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}
		
		
	}
}
