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
			
			
			// Calculate & Adjust for splits
			PreparedStatement industry = conn
					.prepareStatement("select Industry, count(distinct Ticker) as TickerCnt" + 
							" from Company natural join PriceVolume" + 
							" group by Industry" + 
							" order by TickerCnt DESC, Industry");
			
			PreparedStatement industryTickers = conn
					.prepareStatement("select Ticker, min(TransDate), max(TransDate), count(distinct TransDate)" + 
							" from Company natural left outer join PriceVolume" + 
							" where Industry = ? " + 
							" group by Ticker" + 
							" order by Ticker");
			
			ResultSet rs = industry.executeQuery();
			
			while (rs.next()) { 
				industryTickers.setString(1, rs.getString(1));
				ResultSet rs2 = industryTickers.executeQuery();
				System.out.println("Industry: " + rs.getString(1) + " " + rs.getDouble(2));

				while (rs2.next()) { //for each Industry, adjust prices for splits
					showTicker(rs2.getString(1));
					CalculatePrices();
				}
			}
			
			System.out.println("done with while loop");
			
			industry.close();
			industryTickers.close();
			
			conn.close();
		} catch (SQLException ex) {
			System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(),
					ex.getErrorCode());
		}
		
		
	}
	
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
//			System.out.printf("%s%n", rs.getString(1));
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
						totalDivisor *= 2.0;
					} else if (split == "3:1") {
						totalDivisor *= 3.0;
					} else if (split == "3:2") {
						totalDivisor *= 1.5;
					}
					co_data.addSplitDay(dayPrices.get(d).getDate() + "\t" + dayPrices.get(d).getClose() + " --> "
							+ dayPrices.get(d - 1).getOpen(), split);
				}
				dayPrices.get(d - 1).updatePrices(totalDivisor);
			}
		}
//		System.out.println("total div: " + totalDivisor);
//		co_data.getSplitDays();
//		 System.out.println("total div: " + totalDivisor);
//		 for (int d = 0; d < daySize - 1; d++) {
//		
//		 System.out.println(dayPrices.get(d).getDate() + " open: " +
//		 dayPrices.get(d).getOpen() + " close: " + dayPrices.get(d).getClose());
//		
//		 }
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
}
			