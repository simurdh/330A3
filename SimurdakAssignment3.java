import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

class SimurdakAssignment3 {

	static Connection conn = null;
	static Connection write = null;

	static ArrayList<Prices> dayPrices;
	static CompanyData co_data;

	public static void main(String[] args) throws Exception {
		// Get connection properties
		String paramsFile = "readerparams.txt";
		if (args.length >= 1) {
			paramsFile = args[0];
		}
		Properties connectprops = new Properties();
		connectprops.load(new FileInputStream(paramsFile));

		String writeFile = "writerparams.txt";
		Properties writeprops = new Properties();
		writeprops.load(new FileInputStream(writeFile));

		try {
			// Get connection
			Class.forName("com.mysql.jdbc.Driver");
			String dburl = connectprops.getProperty("dburl");
			String username = connectprops.getProperty("user");
			conn = DriverManager.getConnection(dburl, connectprops);
			System.out.printf("Database connection %s %s established.%n", dburl, username);

			String dburl2 = writeprops.getProperty("dburl");
			String username2 = writeprops.getProperty("user");
			write = DriverManager.getConnection(dburl2, writeprops);
			System.out.printf("Database connection %s %s established.%n", dburl2, username2);

			// Enter Ticker and optional start end dates, Fetch data for that ticker and
			// dates
			Scanner in = new Scanner(System.in);
			while (true) {
				System.out.print("Enter ticker and date (YYYY.MM.DD): ");
				String[] data = in.nextLine().trim().split("\\s+");

				if (data.length == 1) {
					if (data[0].equals("")) {
						System.out.println("Database connection closed.");
						break;
					} else {
						showTicker(data[0]);
						CalculatePrices();
					}

				} else if (data.length == 3) {
					if (showTickerDays(data[0], data[1], data[2]) == 0) {
						CalculatePrices();
					}
//					StockCompare();
				}
			}

			conn.close();
		} catch (SQLException ex) {
			System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(),
					ex.getErrorCode());
		}
	}

	/* When no dates are specified */
	static int showTicker(String ticker) throws SQLException {
		// Prepare query
		PreparedStatement name = conn
				.prepareStatement("select name, Ticker" + "	from company " + "   where Ticker = ?");

		// PV Data Columns: 1) Ticker 2) TransDate 3) OpenPrice 4)HighPrice 5) LowPrice
		// 6) ClosePrice 7) Volume
		ResultSet rs2 = null;
		final String getData = "select Ticker, TransDate, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, AdjustedClose"
				+ "	from PriceVolume " + "   where Ticker = ? " + "   order by TransDate DESC";
		PreparedStatement PVdata = conn.prepareStatement(getData, ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_UPDATABLE);

		name.setString(1, ticker);
		ResultSet rs = name.executeQuery();

		PVdata.setString(1, ticker);
		rs2 = PVdata.executeQuery();

		if (rs.next()) {
			System.out.printf("%s%n", rs.getString(1));
			co_data = new CompanyData(rs.getString(1));
			dayPrices = new ArrayList<Prices>();
			while (rs2.next()) {
				Prices curDay = new Prices(rs2.getString(2));
				curDay.addDay(rs2.getDouble(3), rs2.getDouble(4), rs2.getDouble(5), rs2.getDouble(6));
				dayPrices.add(curDay);
				co_data.countDay();
			}

		} else {
			System.out.printf("%s not found in database.\n%n", ticker);
			return -1;
		}
		name.close();
		PVdata.close();
		return 0;
	}

	/* When dates are specified */
	static int showTickerDays(String ticker, String start_date, String end_date) throws SQLException {

		// Prepare query
		PreparedStatement name = conn
				.prepareStatement("select name, Ticker" + "	from company " + "   where Ticker = ?");

		PreparedStatement PVdata1 = conn.prepareStatement("select *" + "	from PriceVolume "
				+ "   where Ticker = ? and TransDate between ? and ? " + "   order by TransDate DESC");

		// PV Data Columns: 1) Ticker 2) TransDate 3) OpenPrice 4)HighPrice 5) LowPrice
		// 6) ClosePrice 7) Volume

		name.setString(1, ticker);
		ResultSet rs = name.executeQuery();

		PVdata1.setString(1, ticker);
		PVdata1.setString(1, ticker);
		PVdata1.setString(2, start_date);
		PVdata1.setString(3, end_date);

		ResultSet rs2 = PVdata1.executeQuery();

		if (rs.next()) {
			System.out.printf("%s%n", rs.getString(1));
			co_data = new CompanyData(rs.getString(1));
			dayPrices = new ArrayList<Prices>();
			while (rs2.next()) {
				Prices curDay = new Prices(rs2.getString(2));
				curDay.addDay(rs2.getDouble(3), rs2.getDouble(4), rs2.getDouble(5), rs2.getDouble(6));
				dayPrices.add(curDay);
				co_data.countDay();
			}

		} else {
			System.out.printf("%s not found in database.\n%n", ticker);
			return -1;

		}
		name.close();
		PVdata1.close();
		return 0;
	}

	/* Calculate Price data */
	static void CalculatePrices() {
		int daySize = dayPrices.size() - 1;
		String split = null;
		int totalDivisor = 1;

		// System.out.println("size: " + daySize);

		for (int d = 0; d < daySize - 1; d++) {

			// System.out.println(dayPrices.get(d).getDate() + " open: " +
			// dayPrices.get(d).getOpen() + " close: " + dayPrices.get(d).getClose());
			if (d > 0) {
				split = calcSplitDay(dayPrices.get(d).getClose(), dayPrices.get(d - 1).getOpen());
				if (split != null) {
					if (split == "2:1") {
						System.out.println("multiplying divisor by 2");
						totalDivisor *= 2.0;
					} else if (split == "3:1") {
						System.out.println("multiplying divisor by 3");
						totalDivisor *= 3.0;
					} else if (split == "3:2") {
						System.out.println("multiplying divisor by 1.5");
						totalDivisor *= 1.5;
					}
					co_data.addSplitDay(dayPrices.get(d).getDate() + "\t" + dayPrices.get(d).getClose() + " --> "
							+ dayPrices.get(d - 1).getOpen(), split);
				}
				dayPrices.get(d - 1).updatePrices(totalDivisor);
			}
		}
		System.out.println("total div: " + totalDivisor);
		co_data.getSplitDays();
		 System.out.println("total div: " + totalDivisor);
		 for (int d = 0; d < daySize - 1; d++) {
		
		 System.out.println(dayPrices.get(d).getDate() + " open: " +
		 dayPrices.get(d).getOpen() + " close: " + dayPrices.get(d).getClose());
		
		 }
	}

	/* returns a string of the split or null if not */
	public static String calcSplitDay(Double C, Double O) {
		// System.out.println("close = " + C + " open = " + O);
		double trysplit = Math.abs(C / O - 3.0);
		// System.out.println("trySplit: " + trysplit);
		if (Math.abs(C / O - 2.0) < 0.20) {
			// System.out.println("split! ");
			return "2:1";
		} else if (Math.abs(C / O - 3.0) < 0.30) {
			// System.out.println("split!\n");
			return "3:1";
		} else if (Math.abs(C / O - 1.5) < 0.15) {
			// System.out.println("split!\n");
			return "3:2";
		} else {
			return null;
		}
	}

	public static void StockCompare() throws SQLException  {
		// Drop table if it exists
		// Prepare query
		Statement stmt = write.createStatement();

		static final String dropPerformanceTable = "drop table if exists Performance; ;

		static final String createPerformanceTable = "create table Performance (Industry char(30), Ticker char(6), StartDate char(10), EndDate char(10), TickerReturn char(12), IndustryReturn char(12));";

		static final String insertPerformance = "insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn) values(?, ?, ?, ?, ?, ?);";                                     

		stmt.execute(dropPerformanceTable);

		stmt.execute(createPerformanceTable );
		
		// Prepare query
		PreparedStatement drop = write
				.prepareStatement("drop table if exists Performance");

			
			drop.executeQuery();
				
			// Prepare query
			PreparedStatement drop = write
					.prepareStatement("drop table if exists Performance");

	}

}