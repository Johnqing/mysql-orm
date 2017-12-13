import {createPool, createConnection} from 'mysql';

function formatSql(sql) {
	return sql.replace(/\s{2,}/g, ' ')
}

/**
 * mysql类
 *
 * {
 *      transaction: true, // 是否支持事物
 *      isPool: true, //是否支持连接池
 *      router: true, //是否支持router,
 *      comments: '备注'
 * }
 *
 *
 */
export default class Mysql {
	constructor(options = {}) {
		this.connect = {
			comments: options.comments || options.host,
			host: options.host,
			user: options.username,
			password: options.password,
			database: options.database,
			port: options.port || 3306,
			dateStrings: options.dateStrings || 'DATE'
		};

		this.pool = options.isPool ? createPool(this.connect) : null;
	}

	/**
	 * 链接池的方式链接数据库
	 * @param sql      sql语句
	 * @param config    {data: []} data:数据项
	 * @returns {Promise}
	 */
	queryPool(sql, config = {}) {
		return new Promise((resolve, reject) => {
			let query;
			this.pool.getConnection((err, conn) => {
				if (err) {
					reject(err);
				} else {
					query = conn.query(sql, config.data || [], function (err, rows, fields) {
						if (err) {
							reject(err);
							return
						}
						let result = JSON.stringify(rows);
						resolve(JSON.parse(result));
						conn.release();
					});
				}
			});
		});
	}

	/**
	 * 客户端链接
	 * @param sql       sql语句
	 * @param config    {data: []} 数据
	 * @param connConfig   数据库链接，可选参数
	 * @returns {Promise}
	 */
	queryClient(sql, config, connConfig) {
		return new Promise((resolve, reject) => {
			let connection = createConnection(connConfig || this.connect);
			connection.connect();
			let query = connection.query(sql, config.data || [], function (err, rows, fields) {
				if (err) {
					reject(err);
					return
				}
				let result = JSON.stringify(rows);
				resolve(JSON.parse(result), fields);
			});

			connection.end();
		})
	}

	/**
	 * 事物提交
	 * @param config  [{sql: `select * from aa`, data: []}] 同一个事物，按照顺序写入
	 * @returns {Promise}
	 */
	queryTransaction(config = []) {

		return new Promise((resolve, reject) => {
			let connection = mysql.createConnection(this.connect);
			connection.connect();

			connection.beginTransaction(function (err) {
				if (err) {
					reject(err);
					return;
				}
				let query;

				// 回滚
				function rollback(err) {
					return connection.rollback(function () {
						reject(err);
					});
				}

				function commint(query, rows) {
					return connection.commit(function (err) {
						if (err) {
							return rollback(err);
						}
						let result = JSON.stringify(rows);
						resolve(JSON.parse(result));
						connection.end();
					});
				}

				/**
				 * 轮询
				 */
				function selectQuery() {
					const sqlConfig = config.shift();
					let sqlStr = formatSql(sqlConfig.sql);
					query = connection.query(sqlStr, sqlConfig.data || [], function (err, rows) {
						if (err) {
							// 事物回滚
							return rollback(err);
						}
						if (!config.length) {
							return commint(query, rows);
						}
						selectQuery()
					});
				}

				selectQuery();
			});
		})
	}

	/**
	 * 数据库路由支持
	 * @param options       {routerSql: 'select * from aa', routerData: [], sql: 'select * from bb', data: []}
	 * 通过路由表，查询不同数据库实例/db的数据。路由表的字段必须为{host:'', user: 'xx', password: 'xx', port: 3306, database: 'aa', comments: 'xx'}
	 * @returns {Promise.<T>}
	 */
	queryRouter(options = {}){
		return this.queryClient(options.routerSql, options.routerData, this.connect).then((res)=> {
			let conn = res[0];
			// 新酒店未建立路由的情况
			if (!conn) {
				return []
			}

			let connConfig = {
				host: conn.host,
				user: conn.user,
				password: conn.password,
				port: conn.port || 3306,
				database: conn.database,
				comments: conn.comments,
				dateStrings: 'DATE'
			};
			return this.queryClient(options.sql, {
				data: options.data
			}, connConfig);
		});
	}

	/**
	 * 通用方法根据不同的选项，切换不同的类型
	 * @param arg
	 * @returns {*}
	 */
	query(...arg){
		if(this.transaction){
			return this.queryTransaction(...arg);
		}

		if(this.pool){
			return this.queryPool(...arg);
		}

		if(this.router){
			return this.queryRouter(...arg);
		}

		return this.queryClient(...arg);
	}
}
