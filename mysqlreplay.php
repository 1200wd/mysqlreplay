#!/usr/bin/php
<?

function main()
{
	$opts = getopt('f::s::u::p::');

	if ( ! $opts)
	{
		print('Usage: ./mysqlreplay.php -fmysql.log -s127.0.0.1 -uusername -ppass');
		die();
	}

	$file = $opts['f'];
	$server = $opts['s'];
	$user = $opts['u'];
	$pass = $opts['p'];

	$adapter = new Mysqlpdo();
	$replay = new Mysqlreplay($user, $pass, $server);
	$replay->set_status_update(function($message) { echo $message; });
	$replay->start($file, $adapter);
}

class Mysqlreplay {

	private $file = null;

	private $server = null;

	private $user = null;

	private $pass = null;

	private $file_handle = null;

	private $connections = array();

	private $adapter = null;

	private $status_update_func = null;

	public function __construct($user, $pass, $server = '127.0.0.1')
	{
		$this->user = $user;
		$this->pass = $pass;
		$this->server = $server;
	}

	public function set_status_update($func)
	{
		$this->status_update_func = $func;
	}

	private function status_update($message)
	{
		if ( ! is_callable($this->status_update_func))
			return;

		$func = $this->status_update_func;
		$func($message);
	}

	public function start($file, Mysqladapter $adapter)
	{
		set_time_limit(0);
		gc_enable();

		$this->adapter = $adapter;
		$this->file_handle = fopen($file, 'r');

		if ($this->file_handle === false)
			throw new Exception('Unable to open file ' . $file);

		// skip until we get to some data
		while ( ! feof($this->file_handle))
		{
			$line = stream_get_line($this->file_handle, 4096, "\n");
			if (substr($line, 0, 4) == 'Time')
			{
				break;
			}
		}

		$this->loop();

		fclose($this->file_handle);
	}

	private function loop()
	{
		while ( ! feof($this->file_handle))
		{
			$line = stream_get_line($this->file_handle, 4096, "\n");
			$info = $this->parse_line($line);

			if ( ! $info)
			{
				$this->status_update('E');
				continue;
			}

			try
			{
				$this->execute($info);
			}
			catch (Exception $e)
			{
				//$this->status_update("\nError: " . $e->getMessage() . "\n");
			}
		}
	}

	private function parse_line($line)
	{
		$return = array();

		$line = trim($line);
		$query = explode("\t", $line, 3);

		if (count($query) == 3)
		{
			 //remove timestamp
			array_shift($query);
		}

		$parts = explode(" ", $query[0], 2);

		$info['command'] = array_pop($parts);
		$info['connection'] = array_pop($parts);

		if ( ! array_key_exists(1, $query))
			return $info;

		$info['query'] = $query[1];

		return $info;
	}

	private function execute($info)
	{
		$conn = $this->get_connection($info['connection']);

		switch ($info['command'])
		{
		case 'Quit':
			$this->status_update('x');
			$conn->quit();
			$this->connections[$info['connection']] = null;
			gc_collect_cycles();
			break;
		case 'Query':
			$this->status_update('q');
			$conn->query($info['query']);
			break;
		case 'Init DB':
			$this->status_update('i');
			$conn->init_db($info['query']);
			break;
		case 'Connect':
			$this->status_update('c');
			// nothing, get_connection() already connects
			break;
		}
	}

	private function get_connection($id)
	{
		// return existing connection
		if (array_key_exists($id, $this->connections))
			return $this->adapter->handle($this->connections[$id]);

		// make new connection
		$this->connections[$id] = $this->adapter->connect($this->user, $this->pass, $this->server);

		return $this->adapter;
	}
}

interface Mysqladapter {
	public function handle($handle);
	public function connect($user, $pass, $server);
	public function init_db($db);
	public function query($query);
	public function quit();
}

class Mysqlpdo implements Mysqladapter {

	private $handle = null;

	public function handle($handle)
	{
		$this->handle = $handle;
		return $this;
	}

	public function connect($user, $pass, $server)
	{
		$pdo = new PDO('mysql:host=' . $server, $user, $pass, array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));
		$pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

		$this->handle = $pdo;
		return $pdo;
	}

	public function init_db($db)
	{
		$statement = $this->handle->prepare('use ' . $db);
		$statement->execute();
	}

	public function query($query)
	{
		$statement = $this->handle->prepare($query);
		$statement->execute();
		$i = 0;
		while ($statement->fetch())
		{
			// wait
		}
	}

	public function quit()
	{
		$this->handle = null;
	}

}

main();
