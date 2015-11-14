<?php

namespace Nextras\Dbal\Drivers\PdoPg;

use Nextras\Dbal\Drivers\IResultAdapter;
use Nextras\Dbal\InvalidArgumentException;
use Nextras\Dbal\InvalidStateException;
use PDO;
use PDOStatement;


class PdoPgResultAdapter implements IResultAdapter
{

	/**
	 * @var array
	 * @see http://www.postgresql.org/docs/9.4/static/datatype.html
	 */
	protected static $types = [
		'bool'        => self::TYPE_DRIVER_SPECIFIC,
		'bit'         => self::TYPE_DRIVER_SPECIFIC,
		'varbit'      => self::TYPE_DRIVER_SPECIFIC,
		'bytea'       => self::TYPE_DRIVER_SPECIFIC,
		'interval'    => self::TYPE_DRIVER_SPECIFIC,
		'time'        => self::TYPE_DRIVER_SPECIFIC,
		'date'        => 33, // self::TYPE_DRIVER_SPECIFIC | self::TYPE_DATETIME,
		'timestamp'   => 33, // self::TYPE_DRIVER_SPECIFIC | self::TYPE_DATETIME,

		'int8'        => self::TYPE_INT,
		'int4'        => self::TYPE_INT,
		'int2'        => self::TYPE_INT,

		'numeric'     => self::TYPE_FLOAT,
		'float4'      => self::TYPE_FLOAT,
		'float8'      => self::TYPE_FLOAT,

		'timetz'      => self::TYPE_DATETIME,
		'timestamptz' => self::TYPE_DATETIME,
	];

	/** @var PDOStatement */
	private $statement;

	/** @var NULL|int */
	private $cursorIndex;


	public function __construct(PDOStatement $statement)
	{
		$this->statement = $statement;

		if (PHP_INT_SIZE < 8) {
			self::$types['int8'] = self::TYPE_DRIVER_SPECIFIC;
		}
	}


	public function __destruct()
	{
		$this->statement->closeCursor();
	}


	public function seek($index)
	{
		$rows = $this->statement->rowCount();
		if ($rows && $index >= $rows) {
			throw new InvalidStateException("Cursor overflow: $index >= $rows.");
		}
		$this->cursorIndex = $index;
	}


	public function fetch()
	{
		if ($this->cursorIndex !== NULL) {
			return $this->statement->fetch(PDO::FETCH_ASSOC, PDO::FETCH_ORI_ABS, $this->cursorIndex++) ?: NULL;
		}
		return $this->statement->fetch(PDO::FETCH_ASSOC, PDO::FETCH_ORI_NEXT) ?: NULL;
	}


	public function getTypes()
	{
		$types = [];
		$count = $this->statement->columnCount();

		for ($i = 0; $i < $count; $i++) {
			$meta = $this->statement->getColumnMeta($i);
			$nativeType = $meta['native_type'];
			$types[$meta['name']] = [
				0 => isset(self::$types[$nativeType]) ? self::$types[$nativeType] : self::TYPE_AS_IS,
				1 => $nativeType,
			];
		}

		return $types;
	}

}
