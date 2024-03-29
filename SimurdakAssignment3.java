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
	
	static final String dropPerformanceTable = "drop table if exists Performance; ";
	static final String createPerformanceTable = "create table Performance (Industry char(30), Ticker char(6), StartDate char(10), EndDate char(10), TickerReturn char(12), IndustryReturn char(12));";   
	static final String insertPerformance = " insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn) values(?, ?, ?, ?, ?, ?);";
	
	String writeFile = "writerparams.txt";
	Properties writeprops = new Properties();



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
			System.out.printf("Reader connection established: Database connection %s %s established.%n", dburl, username);

			String dburl2 = writeprops.getProperty("dburl");
			String username2 = writeprops.getProperty("user");
			write = DriverManager.getConnection(dburl2, writeprops);
			System.out.printf("Writer connection established: Database connection %s %s established.%n", dburl2, username2);
			
			
			// Prepared statements

			Statement stmt = write.createStatement();
			stmt.execute(dropPerformanceTable);
			stmt.execute(createPerformanceTable );
			
			PreparedStatement insert = write
					.prepareStatement("insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn) values(?, ?, ?, ?, ?, ?)");
			
			PreparedStatement industry = conn
					.prepareStatement("select Industry, count(distinct Ticker) as TickerCnt " + 
							" from Company natural join PriceVolume" + 
							" group by Industry" + 
							" order by TickerCnt DESC, Industry");
			
			PreparedStatement industryN = conn
					.prepareStatement("select Industry " + 
							" from Company natural join PriceVolume" + 
							" group by Industry" + 
							" order by Industry");
			
			PreparedStatement industryTickers = conn
					.prepareStatement("select Ticker, min(TransDate), max(TransDate), count(distinct TransDate) as TradingDays" + 
							" from Company natural left outer join PriceVolume" + 
							" where Industry = ? " + 
							" and TransDate >= ? and TransDate <= ? " +
							" group by Ticker" + 
							" having TradingDays >= 150" +
							" order by Ticker");
			
			PreparedStatement max_min_TransDate = conn
					.prepareStatement("select max(minDates) from" + 
							"(select min(TransDate) as minDates, count(distinct TransDate) as TradingDays" + 
							"							from Company natural left outer join PriceVolume" + 
							"							where Industry = ? " + 
							"							group by Ticker" +
							" 							having TradingDays >= 150 " +
							"							order by Ticker) as minDates");
			
			PreparedStatement min_max_TransDate = conn
					.prepareStatement("select min(maxDates) from" + 
							"(select max(TransDate) as maxDates, count(distinct TransDate) as TradingDays" + 
							"							from Company natural left outer join PriceVolume" + 
							"							where Industry = ? " + 
							"							group by Ticker" +
							" 							having TradingDays >= 150 " +
							"							order by Ticker) as maxDates");
			
			PreparedStatement min_tradingDays = conn
					.prepareStatement("select min(TradingDays) from" + 
							" (select Ticker, min(TransDate), max(TransDate), count(distinct TransDate) as TradingDays" + 
							" from Company natural join PriceVolume" + 
							" where Industry = ? " + 
							" and TransDate >= ? and TransDate <= ?" + 
							" group by Ticker" + 
							" having TradingDays >= 150" + 
							" order by Ticker) as TradingDays");
			
			PreparedStatement DataRange = conn
					.prepareStatement(" select P.TransDate" + 
							" from PriceVolume P" + 
							" where Ticker = ? and TransDate >= ? and TransDate < ? ");
			
			PreparedStatement LastDataRange = conn
					.prepareStatement(" select P.TransDate" + 
							" from PriceVolume P" + 
							" where Ticker = ? and TransDate >= ? and TransDate <= ? ");

			ResultSet rs = industry.executeQuery();
			ResultSet print = industryN.executeQuery();
			
			int countIndustry = 0;
			ArrayList<String> toPrint = new ArrayList<String>();
			while (print.next()) {
				countIndustry++;
				toPrint.add(print.getString(1));
			}
			System.out.println(countIndustry + " industries found");
			for (int n = 0; n < countIndustry; n++) {
				System.out.println(toPrint.get(n));
			}
			System.out.println("");
			
			// For each Industry
			while (rs.next()) {
				
				String industryName = rs.getString(1);
				System.out.println("Processing " + industryName);
				
				//Find max(min(TransDate)
				max_min_TransDate.setString(1, rs.getString(1));
				ResultSet result = max_min_TransDate.executeQuery();
				String max_minDate = null;
				if (result.next()) {
					max_minDate = result.getString(1);
				} 
				
				//Find min(max(TransDate)
				min_max_TransDate.setString(1, rs.getString(1));
				ResultSet result2 = min_max_TransDate.executeQuery();
				String min_maxDate = null;
				if (result2.next()) {
					min_maxDate = result2.getString(1);
				}
				System.out.println(rs.getInt(2) + " accepted tickers for " + industryName + "(" + max_minDate + " - " + min_maxDate + "), \n");

				industryTickers.setString(1, rs.getString(1));
				industryTickers.setString(2, max_minDate);
				industryTickers.setString(3, min_maxDate);
				ResultSet rs2 = industryTickers.executeQuery();
				
				// Get trading intervals
				min_tradingDays.setString(1, rs.getString(1));
				min_tradingDays.setString(2, max_minDate);
				min_tradingDays.setString(3, min_maxDate);
				ResultSet numDays = min_tradingDays.executeQuery();
				int numIntervals = 0;
				if (numDays.next()) {
					numIntervals = numDays.getInt(1);
				}
				numIntervals = numIntervals/60;
				
				// First Ticker
				String firstTicker = null;
				int i = 0;
				LinkedHashMap<Integer, String> IntervalStartDates = new LinkedHashMap<>();
				LinkedHashMap<Integer, String> IntervalEndDates = new LinkedHashMap<>();
				ArrayList<CompanyData> Tickers = new ArrayList<CompanyData>();
				
				// For each Ticker
					while (rs2.next()) { 
						
						if (i == 0) {
							firstTicker = rs2.getString(1);
							// Store all start and end dates of the intervals for the first ticker
							DataRange.setString(1, firstTicker);
							DataRange.setString(2, max_minDate);
							DataRange.setString(3, min_maxDate);
							ResultSet firstTickerRange = DataRange.executeQuery();
							
							int dayNum = 1;
							int j = 1;		
							
							while (firstTickerRange.next()) {
								// Get start date of interval
								if (dayNum == 1 + ((j - 1)*60)) {
									String intervalStart = firstTickerRange.getString(1);
									IntervalStartDates.put(j, intervalStart);
									// Get end date of interval	
								} else if (dayNum == (j*60)) {
									String intervalEnd = firstTickerRange.getString(1);							
									IntervalEndDates.put(j, intervalEnd);
									j++; //another interval accounted for
								}
								dayNum++;
							}					
						}
					
						// adjust prices for that ticker and save in an ArrayList of day prices
						CompanyData cur_co_data = CalculatePrices(showTickerDays(rs2.getString(1), max_minDate, min_maxDate));
						
						
						// For each Interval
						for (int k = 1; k <= numIntervals; k++) {

							if (i == 0) {
								cur_co_data.addStartDate(k, IntervalStartDates.get(k));
								cur_co_data.addEndDate(k, IntervalEndDates.get(k));

							} else {								
								if (k < numIntervals) {
									// Get dates of interval for this ticker and interval
									DataRange.setString(1, rs2.getString(1));
									DataRange.setString(2, IntervalStartDates.get(k));
									DataRange.setString(3, IntervalStartDates.get(k + 1));
									ResultSet startDate = DataRange.executeQuery();
									
									int count = 0;
									while (startDate.next()) {
										if (count == 0) { //start date of interval
											cur_co_data.addStartDate(k, startDate.getString(1));
										}
										count++;
										if (startDate.isLast()) { // end date of interval
											cur_co_data.addEndDate(k, startDate.getString(1));											
										}
									}
										
								} else { //last interval
									// Get dates of interval for this ticker and interval
									LastDataRange.setString(1, rs2.getString(1));
									LastDataRange.setString(2, IntervalStartDates.get(k));
									LastDataRange.setString(3, IntervalEndDates.get(k));
									ResultSet startDate = DataRange.executeQuery();

									
									while (startDate.next()) {
										if (startDate.isFirst()) {
											cur_co_data.addStartDate(k, startDate.getString(1));

										}
										if (startDate.isLast()) {
											cur_co_data.addEndDate(k, startDate.getString(1));
										}
									}
								}
							}
						}
						Tickers.add(cur_co_data);
						i++;
						
					}
					int m = Tickers.size();
					
					// Calculate Industry and Ticker Returns //
					
					// for each ticker
					for (int x = 0; x < m; x++) {
						ArrayList<Integer> TickersIndex = new ArrayList<Integer>(); //list of indexes into Tickers for all companies except X
						for (int ix = 0; ix < Tickers.size(); ix++) {
							if (!Tickers.get(ix).getCompany().equals(Tickers.get(x).getCompany())) {
								TickersIndex.add(ix); 
							}
						}
						// for each interval
						for (int y = 1; y <= numIntervals; y++) {
							String start = Tickers.get(x).getIntervalStartDate(y);
							String end = Tickers.get(x).getIntervalEndDate(y);
							
							Double tickerReturn = calcTickerReturn(Tickers.get(x).getPrices(start), Tickers.get(x).getPrices(end));
							
							
							
							//Calculate Industry Return
							Double sum = 0.0;
							for (int k = 0; k < m - 1; k++) {
								
								String closeDay = Tickers.get(TickersIndex.get(k)).getIntervalEndDate(y);
								Prices closeP =  Tickers.get(TickersIndex.get(k)).getPrices(closeDay);
								
								String startDay = Tickers.get(TickersIndex.get(k)).getIntervalStartDate(y);
								Prices openP =  Tickers.get(TickersIndex.get(k)).getPrices(startDay);
								
								sum += (closeP.getClose() / openP.getOpen());
								
							}
							Double industryReturn = ((1.0/(m-1)) * sum) - 1.0;
							
							// Write output to simurdh database
							String tickerRet = String.format("%10.7f",  tickerReturn);
							String industryRet = String.format("%10.7f",  industryReturn);
							
							insert.setString(1, industryName); // Industry
							insert.setString(2, Tickers.get(x).getCompany()); // Ticker ??????
							insert.setString(3, start); // StartDate
							insert.setString(4, end); // EndDate
							insert.setString(5, tickerRet); // TickerReturn
							insert.setString(6, industryRet); // IndustryReturn
							
							insert.executeUpdate();
						}
					}
			}
			
			insert.close();
			industryN.close();
			industry.close();
			industryTickers.close();
			max_min_TransDate.close();
			min_max_TransDate.close();
			min_tradingDays.close();
			DataRange.close();
			LastDataRange.close();		
			
			conn.close();
			write.close();
			
			System.out.println("Database connections closed");
			
		} catch (SQLException ex) {
			System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(),
					ex.getErrorCode());
		}	
	}
	
	
	/* returns a co_data object */
	static CompanyData showTickerDays(String ticker, String start_date, String end_date) throws SQLException {
		
		CompanyData co_data = new CompanyData(ticker);
		
		PreparedStatement PVdata1 = conn.prepareStatement("select *" + "	from PriceVolume "
				+ "   where Ticker = ? and TransDate between ? and ? " + "   order by TransDate DESC");

		// PV Data Columns: 1) Ticker 2) TransDate 3) OpenPrice 4)HighPrice 5) LowPrice
		// 6) ClosePrice 7) Volume


		PVdata1.setString(1, ticker);
		PVdata1.setString(1, ticker);
		PVdata1.setString(2, start_date);
		PVdata1.setString(3, end_date);

		ResultSet rs2 = PVdata1.executeQuery();

			while (rs2.next()) {
				
				Prices curDay = new Prices(rs2.getString(2));
				curDay.addDay(rs2.getDouble(3), rs2.getDouble(4), rs2.getDouble(5), rs2.getDouble(6));
				co_data.addDay(curDay, rs2.getString(2));
				co_data.countDay();
			}

		PVdata1.close();
		return co_data;
	}
	
	/* Calculate Price data: returns co_data */
	static CompanyData CalculatePrices(CompanyData co_data) {
		ArrayList<Prices> dayPrices = co_data.getDayPrices();
		
		int daySize = dayPrices.size() - 1;
		String split = null;
		double totalDivisor = 1.0;

		for (int d = 0; d < daySize - 1; d++) {

			if (d > 0) {
				split = calcSplitDay(dayPrices.get(d).getClose(), dayPrices.get(d - 1).getOpen());
				if (split != null) {
					if (split.equals("2:1")) {
						totalDivisor *= 2.0;
					} else if (split.equals("3:1")) {
						totalDivisor *= 3.0;
					} else if (split.equals("3:2")) {
						totalDivisor *= 1.5;
					}
					co_data.addSplitDay(dayPrices.get(d).getDate() + "\t" + dayPrices.get(d).getClose() + " --> "
							+ dayPrices.get(d - 1).getOpen(), split);
				}

				dayPrices.get(d - 1).updateClose(totalDivisor);
				dayPrices.get(d - 1).updateOpen(totalDivisor);
				
				if (d == (daySize - 1)) {
					dayPrices.get(d).updateClose(totalDivisor);
					dayPrices.get(d).updateOpen(totalDivisor);
				}
			}
		}
		return co_data;
	}

	/* returns a string of the split or null if not */
	public static String calcSplitDay(Double C, Double O) {
		if (Math.abs(C / O - 2.0) < 0.20) {
			return "2:1";
		} else if (Math.abs(C / O - 3.0) < 0.30) {
			return "3:1";
		} else if (Math.abs(C / O - 1.5) < 0.15) {
			return "3:2";
		} else {
			return null;
		}
	}
	
	/* returns the TickerReturn for given prices */
	public static Double calcTickerReturn(Prices startDay, Prices endDay) {
		Double result = endDay.getClose() / startDay.getOpen();
		return (result - 1);
		
	}
}
	


