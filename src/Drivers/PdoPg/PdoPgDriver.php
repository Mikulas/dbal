<?php

namespace Nextras\Dbal\Drivers\PdoPg;

use DateInterval;
use DateTimeZone;
use Nextras\Dbal\Connection;
use Nextras\Dbal\ConnectionException;
use Nextras\Dbal\DriverException;
use Nextras\Dbal\Drivers\PdoDriver;
use Nextras\Dbal\ForeignKeyConstraintViolationException;
use Nextras\Dbal\InvalidArgumentException;
use Nextras\Dbal\NotNullConstraintViolationException;
use Nextras\Dbal\NotSupportedException;
use Nextras\Dbal\Platforms\IPlatform;
use Nextras\Dbal\Platforms\PostgreSqlPlatform;
use Nextras\Dbal\QueryException;
use Nextras\Dbal\Result\Result;
use Nextras\Dbal\UniqueConstraintViolationException;
use PDO;


class PdoPgDriver extends PdoDriver
{

	/**
	 * @var NULL|PDO
	 */
	private $connection;

	/**
	 * @var NULL|DateTimeZone
	 */
	private $simpleStorageTz;

	/**
	 * @var NULL|DateTimeZone
	 */
	private $connectionTz;

	/**
	 * @var NULL|int
	 */
	private $affectedRows;


	/**
	 * Connects the driver to database.
	 *
	 * @param  array $params
	 */
	public function connect(array $params)
	{
		static $knownKeys = ['host', 'port', 'dbname', 'user', 'password'];

		$unknown = [];
		$dsnParts = [];
		foreach ($params as $key => $value) {
			if (in_array($key, $knownKeys, TRUE)) {
				$dsnParts[] = "$key=$value";
			}
		}
		if ($unknown) {
			$fmtUnknown = implode("', '", $unknown);
			throw new InvalidArgumentException("Invalid parameter '$fmtUnknown'");
		}
		$dsn = 'pgsql:' . implode(';', $dsnParts);

		$this->translateException(function() use ($dsn) {
			$this->connection = new PDO($dsn);
			$this->connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		});

		// TODO use other $params

		$this->simpleStorageTz = new DateTimeZone($params['simpleStorageTz']);
		$this->connectionTz = new DateTimeZone($params['connectionTz']);
		$this->query('SET TIME ZONE ' . $this->convertToSql($this->connectionTz->getName(), self::TYPE_IDENTIFIER));
	}


	/**
	 * Disconnects from the database.
	 */
	public function disconnect()
	{
		// PDO does not have explicit disconnect
		$this->connection = NULL;
	}


	/**
	 * Returns true, if there is created connection.
	 *
	 * @return bool
	 */
	public function isConnected()
	{
		return $this->connection !== NULL;
	}


	/**
	 * Returns connection resource.
	 *
	 * @return PDO|NULL
	 */
	public function getResourceHandle()
	{
		return $this->connection;
	}


	/**
	 * Runs query and returns the result. Returns NULL if query does not select data.
	 *
	 * @param  string $query
	 * @return Result|NULL
	 */
	public function query($query)
	{
		$time = microtime(TRUE);
		$statement = $this->translateException(function() use ($query) {
			 return $this->connection->query($query);
		}, $query);
		$time = microtime(TRUE) - $time;

		$this->affectedRows = $statement->rowCount();
		return new Result(new PdoPgResultAdapter($statement), $this, $time);
	}


	/**
	 * Returns the last inserted id.
	 *
	 * @param  string|NULL $sequenceName
	 * @return mixed
	 */
	public function getLastInsertedId($sequenceName = NULL)
	{
		if (!$sequenceName) {
			throw new InvalidArgumentException('PgsqlDriver require to pass sequence name for getLastInsertedId() method.');
		}

		$id = $this->connection->lastInsertId($sequenceName);
		if (is_numeric($id) || ctype_digit($id)) {
			return (int) $id;
		}
		return $id;
	}


	/**
	 * Returns number of affected rows.
	 *
	 * @return int
	 */
	public function getAffectedRows()
	{
		return $this->affectedRows;
	}


	/**
	 * Creates database platform.
	 *
	 * @param  Connection $connection
	 * @return IPlatform
	 */
	public function createPlatform(Connection $connection)
	{
		return new PostgreSqlPlatform($connection);
	}


	/**
	 * Returns server version in X.Y.Z format.
	 *
	 * @return string
	 */
	public function getServerVersion()
	{
		return $this->query('SHOW server_version')->fetch();
	}


	/**
	 * Pings server.
	 *
	 * @return bool
	 */
	public function ping()
	{
		// PDO does not support native ping

		// This fails when a transaction fails - for example when doing a
		// serialized transaction and another session preforms an update between
		// the serialized transaction's SELECT and UPDATE. In this situation no
		// SELECTS are allowed until a ROLLBACK.
		$this->query('SELECT 1');
	}


	/**
	 * Begins a transaction.
	 *
	 * @throws QueryException
	 */
	public function beginTransaction()
	{
		$this->connection->beginTransaction();
	}


	/**
	 * Commits a transaction.
	 *
	 * @throws QueryException
	 */
	public function commitTransaction()
	{
		$this->connection->commit();
	}


	/**
	 * Rollback a transaction.
	 *
	 * @throws QueryException
	 */
	public function rollbackTransaction()
	{
		$this->connection->rollBack();
	}


	/**
	 * Converts database value to php boolean.
	 *
	 * @param  string $value
	 * @param  mixed  $nativeType
	 * @return mixed
	 */
	public function convertToPhp($value, $nativeType)
	{
		static $trues = ['true', 't', 'yes', 'y', 'on', '1'];

		if ($nativeType === 'bool') {
			return in_array(strtolower($value), $trues, TRUE);

		} elseif ($nativeType === 'time' || $nativeType === 'date' || $nativeType === 'timestamp') {
			return $value . ' ' . $this->simpleStorageTz->getName();

		} elseif ($nativeType === 'int8') {
			// called only on 32bit
			return is_float($tmp = $value * 1) ? $value : $tmp;

		} elseif ($nativeType === 'interval') {
			return DateInterval::createFromDateString($value);

		} elseif ($nativeType === 'bit' || $nativeType === 'varbit') {
			return bindec($value);

		} elseif ($nativeType === 'bytea') {
			/** @var resource $value */
			return stream_get_contents($value); // TODO

		} else {
			throw new NotSupportedException("PgsqlDriver does not support '{$nativeType}' type conversion.");
		}
	}


	/**
	 * Converts php value to database value.
	 *
	 * @param  mixed $value
	 * @param  mixed $type
	 * @return string
	 */
	public function convertToSql($value, $type)
	{
		switch ($type) {
			case self::TYPE_STRING:
				return $this->connection->quote($value, PDO::PARAM_STR);

			case self::TYPE_BOOL:
				return $value ? 'TRUE' : 'FALSE';

			case self::TYPE_IDENTIFIER:
				$parts = explode('.', $value);
				foreach ($parts as &$part) {
					if ($part !== '*') {
						// @see http://www.postgresql.org/docs/8.2/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
						$part = '"' . str_replace('"', '""', $part) . '"';
					}
				}
				return implode('.', $parts);

			case self::TYPE_DATETIME:
				if ($value->getTimezone()->getName() !== $this->connectionTz->getName()) {
					if ($value instanceof \DateTimeImmutable) {
						$value = $value->setTimezone($this->connectionTz);
					} else {
						$value = clone $value;
						$value->setTimezone($this->connectionTz);
					}
				}
				return $this->convertToSql($value->format('Y-m-d H:i:s'), self::TYPE_STRING);

			case self::TYPE_DATETIME_SIMPLE:
				if ($value->getTimezone()->getName() !== $this->simpleStorageTz->getName()) {
					if ($value instanceof \DateTimeImmutable) {
						$value = $value->setTimezone($this->simpleStorageTz);
					} else {
						$value = clone $value;
						$value->setTimezone($this->simpleStorageTz);
					}
				}
				return $this->convertToSql($value->format('Y-m-d H:i:s'), self::TYPE_STRING);

			case self::TYPE_DATE_INTERVAL:
				return $value->format('P%yY%mM%dDT%hH%iM%sS');

			case self::TYPE_BLOB:
				return $this->connection->quote($value, PDO::PARAM_LOB);

			default:
				throw new InvalidArgumentException();
		}
	}


	/**
	 * Adds driver-specific limit clause to the query.
	 *
	 * @param  string   $query
	 * @param  int|NULL $limit
	 * @param  int|NULL $offset
	 * @return string
	 */
	public function modifyLimitQuery($query, $limit, $offset)
	{
		if ($limit !== NULL) {
			$query .= ' LIMIT ' . (int) $limit;
		}
		if ($offset !== NULL) {
			$query .= ' OFFSET ' . (int) $offset;
		}
		return $query;
	}


	/**
	 * @param callable $scope
	 * @param string   $query
	 * @return mixed
	 * @throws DriverException
	 */
	private function translateException($scope, $query = NULL)
	{
		try {
			return $scope();

		} catch (\PDOException $e) {
			switch ($e->getCode()) {
				case 7:
					throw new ConnectionException($e->getMessage(), $e->getCode(), NULL, $e);
				case 42601:
					throw new QueryException($e->getMessage(), $e->getCode(), NULL, $e, $query);
				case 23502:
					throw new NotNullConstraintViolationException($e->getMessage(), $e->getCode(), NULL, $e);
				case 23505:
					throw new UniqueConstraintViolationException($e->getMessage(), $e->getCode(), NULL, $e);
				case 23503:
					throw new ForeignKeyConstraintViolationException($e->getMessage(), $e->getCode(), NULL, $e);
				default:
					duMP($e->getCode());
					throw $e;
			}
		}
	}

}
