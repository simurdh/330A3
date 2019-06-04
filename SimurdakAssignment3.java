import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.Map.Entry;

class SimurdakAssignment3 {

	static Connection conn = null;
	static Connection write = null;



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
			
			
			// Prepared statments
			PreparedStatement industry = conn
					.prepareStatement("select Industry, count(distinct Ticker) as TickerCnt" + 
							" from Company natural join PriceVolume" + 
							" group by Industry" + 
							" order by TickerCnt DESC, Industry");
			
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
			
			// For each Industry
			while (rs.next()) { 
								
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
				
				industryTickers.setString(1, rs.getString(1));
				industryTickers.setString(2, max_minDate);
				industryTickers.setString(3, min_maxDate);
				ResultSet rs2 = industryTickers.executeQuery();
				System.out.println("Industry: " + rs.getString(1) + " " + rs.getDouble(2));
				System.out.println("start date: " + max_minDate + " end date: " + min_maxDate);
				
				// Get trading intervals
				min_tradingDays.setString(1, rs.getString(1));
				min_tradingDays.setString(2, max_minDate);
				min_tradingDays.setString(3, min_maxDate);
				ResultSet numDays = min_tradingDays.executeQuery();
				int numIntervals = 0;
				if (numDays.next()) {
					numIntervals = numDays.getInt(1);
//					System.out.println("numDays: " + numIntervals);
				}
				numIntervals = numIntervals/60;
				System.out.println("numIntervals = " + numIntervals);
				
				// First Ticker
				String firstTicker = null;
				int i = 0;
				LinkedHashMap<Integer, String> IntervalStartDates = new LinkedHashMap<>();
				LinkedHashMap<Integer, String> IntervalEndDates = new LinkedHashMap<>();
				
				
				// For each Ticker
//				if (rs.getString(1).equals("Telecommunications Services")) {
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
									if (rs2.getString(1).equals("FTR")) {
										System.out.println("start Date: " + intervalStart);
										System.out.println("Interval: " + j);	
									}
						
									IntervalStartDates.put(j, intervalStart);
									// Get end date of interval	
								} else if (dayNum == (j*60)) {
//									System.out.println("DayNum: " + dayNum);
									String intervalEnd = firstTickerRange.getString(1);
//									System.out.println("end Date: " + intervalEnd);
							
									IntervalEndDates.put(j, intervalEnd);
									j++; //another interval accounted for
								}
								dayNum++;
							}					
						}
						if (rs2.getString(1).equals("FTR")) {
							System.out.println("IntervalStartDates = " + IntervalStartDates.entrySet());
						}
			
//						System.out.println("first Ticker: " + firstTicker);
						
						// adjust prices for that ticker and save in an ArrayList of day prices
						CompanyData cur_co_data = CalculatePrices(showTickerDays(rs2.getString(1), max_minDate, min_maxDate));
						
						
						// For each Interval
						for (int k = 1; k <= numIntervals; k++) {
							if (rs2.getString(1).equals("FTR")) {
								System.out.println("k = " + k);

							}
							String startDay = null;
							String endDay = null;
							if (i == 0) {
								startDay = IntervalStartDates.get(k);
								endDay = IntervalEndDates.get(k);
							} else {
//								System.out.println("not first ticker");
								
								if (k < numIntervals) {
									// Get dates of interval for this ticker and interval
									if (rs2.getString(1).equals("FTR")) {
										System.out.println("startDate: " + IntervalStartDates.get(k) + " Interval end date: " + IntervalEndDates.get(k));

									}
									DataRange.setString(1, rs2.getString(1));
									DataRange.setString(2, IntervalStartDates.get(k));
									DataRange.setString(3, IntervalStartDates.get(k + 1));
									ResultSet startDate = DataRange.executeQuery();
									
									int count = 0;
//									System.out.println("before while loop");
									while (startDate.next()) {
//										System.out.println("count = " + count);
										if (count == 0) { //start date of interval
											startDay = startDate.getString(1);
										}
										count++;
										if (startDate.isLast()) { // end date of interval
											endDay = startDate.getString(1);
											if (rs2.getString(1).equals("FTR")) {
												System.out.println("end day set: " + endDay);
											}
										}
									}
										
								} else { //last interval
									if (rs2.getString(1).equals("FTR")) {
										System.out.println("last interval");

									}
									// Get dates of interval for this ticker and interval
									LastDataRange.setString(1, rs2.getString(1));
									LastDataRange.setString(2, IntervalStartDates.get(k));
									LastDataRange.setString(3, IntervalEndDates.get(k));
									ResultSet startDate = DataRange.executeQuery();

									
									while (startDate.next()) {
										if (startDate.isFirst()) {
											startDay = startDate.getString(1);
										}
										if (startDate.isLast()) {
											endDay = startDate.getString(1);
										}
									}
								}
							}
							
						
//							Double industryReturn = calcIndustryReturn(cur_co_data.getPrices(startDay), cur_co_data.getPrices(endDay));
							if (rs2.getString(1).equals("FTR")) {

								System.out.println("StartDay: " + startDay);
								Double tickerReturn = calcTickerReturn(cur_co_data.getPrices(startDay), cur_co_data.getPrices(endDay));

								System.out.println("StartDate: " + startDay + " endDate: " + endDay + " ticker return: " + tickerReturn);
							}
						}
						i++;
						
					}	
//				}

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

//		System.out.println("ticker: " + co_data.getCompany());
//		if (co_data.getCompany().equals("INTC")) {
//			 for (int d = 0; d < daySize - 1; d++) {
//					
//				 System.out.println(dayPrices.get(d).getDate() + " open: " +
//				 dayPrices.get(d).getOpen() + " close: " + dayPrices.get(d).getClose());
//				
//				 }
//		}

		
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
	
//	/* returns the TickerReturn for given prices */
//	public static Double calcIndustryReturn(Prices startDay, Prices endDay) {
//		
//	}
	
	
	
	
	
}
	


