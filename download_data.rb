require 'pp'
require 'csv'
require 'open-uri'
require 'date'
require 'fileutils'
require 'json'
require 'bigdecimal'
 
MonthToMonthCode = (1..12).zip(['F','G','H','J','K','M','N','Q','U','V','X','Z']).to_h    # {1=>"F", 2=>"G", 3=>"H", 4=>"J", 5=>"K", 6=>"M", 7=>"N", 8=>"Q", 9=>"U", 10=>"V", 11=>"X", 12=>"Z"}
MonthCodeToMonth = MonthToMonthCode.invert

DataDir = File.absolute_path(File.join(File.dirname(__FILE__), "data"))
TreasuryBillRateFilePath = File.join(DataDir, "tbill13week.csv")

Today = Date.today

# Get future from CBOE and save to file
# More info: http://cfe.cboe.com/products/historicalvix.aspx:
# month is a 1-based index of the month - 1=January, ..., 12=December
# returns path to downloaded file
def save_vix_future_data(year, month, directory, force_download = false)
  force_download = force_download || year > Today.year || (year == Today.year && month >= Today.month)   # we want to re-download files for contracts that haven't expired yet
  
  month_code = MonthToMonthCode[month]
  year_suffix = year.to_s[-2..-1]
  file_name = "CFE_#{month_code}#{year_suffix}_VX.csv"
  file_path = File.join(directory, file_name)
  
  if File.exists?(file_path) && !force_download
    puts "File #{file_path} already exists. Skipping."
  else
    url = "http://cfe.cboe.com/Publish/ScheduledTask/MktData/datahouse/#{file_name}"

    puts "Downloading #{url}"
    file_contents = open(url).read()
    File.open(file_path, 'w') { |file| file.write(file_contents) }
  end
  
  file_path
rescue => e
  puts e.message
end

TreasuryBill = Struct.new(:auction_date, :issue_date, :maturity_date, :face_value, :purchase_price, :high_price, :low_price, :high_discount_rate, :low_discount_rate, :maturity_in_days)

def save_13_week_treasury_bill_rate
  # download from Yahoo!
  # start_date = Date.new(2005, 1, 1)
  # end_date = Today
  # lines = ["Date,Value"]
  # YahooFinance.get_historical_quotes("^IRX", start_date, end_date) do |row|
  #   # each row is of the form: [date (yyyy-mm-dd), open, high, low, close, volume, adj-close]
  #   date, open, high, low, close, volume, adj_close = *row
  #   lines << "#{date},#{high.to_f / 100.0}"
  # end
  # File.write(TreasuryBillRateFilePath, lines.join("\n"))
  
  # download from US Treasury
  url = 'http://www.treasurydirect.gov/TA_WS/securities/jqsearch?format=json&securityTypeoperator=and&filtervalue0=Bill&filtercondition0=EQUAL&filteroperator0=1&filterdatafield0=securityType&securityTermoperator=and&filtervalue1=13&filtercondition1=CONTAINS&filteroperator1=1&filterdatafield1=securityTerm&filterscount=2&groupscount=0&pagenum=0&pagesize=2000&recordstartindex=0&recordendindex=2000'
  json_string = open(url).read()
  json = JSON.parse(json_string)
  
  # json is a Hash of the form:
  # {"totalResultsCount"=>1833,
  #  "securityList"=>
  #   [
  #     {
  #       "cusip"=>"912796EE5",
  #       "issueDate"=>"2014-08-21T00:00:00",
  #       "securityType"=>"Bill",
  #       "securityTerm"=>"13-Week",
  #       "maturityDate"=>"2014-11-20T00:00:00",
  #       "interestRate"=>"",
  #       "refCpiOnIssueDate"=>"",
  #       "refCpiOnDatedDate"=>"",
  #       "announcementDate"=>"2014-08-14T00:00:00",
  #       "auctionDate"=>"2014-08-18T00:00:00",
  #       "auctionDateYear"=>"2014",
  #       "datedDate"=>"",
  #       "accruedInterestPer1000"=>"",
  #       "accruedInterestPer100"=>"",
  #       "adjustedAccruedInterestPer1000"=>"",
  #       "adjustedPrice"=>"",
  #       "allocationPercentage"=>"61.240000",
  #       "allocationPercentageDecimals"=>"2",
  #       "announcedCusip"=>"",
  #       "auctionFormat"=>"Single-Price",
  #       "averageMedianDiscountRate"=>"0.025000",
  #       "averageMedianInvestmentRate"=>"",
  #       "averageMedianPrice"=>"",
  #       "averageMedianDiscountMargin"=>"",
  #       "averageMedianYield"=>"",
  #       "backDated"=>"",
  #       "backDatedDate"=>"",
  #       "bidToCoverRatio"=>"4.700000",
  #       "callDate"=>"",
  #       "callable"=>"",
  #       "calledDate"=>"",
  #       "cashManagementBillCMB"=>"No",
  #       "closingTimeCompetitive"=>"11:30 AM",
  #       "closingTimeNoncompetitive"=>"11:00 AM",
  #       "competitiveAccepted"=>"28446079000",
  #       "competitiveBidDecimals"=>"3",
  #       "competitiveTendered"=>"135651339000",
  #       "competitiveTendersAccepted"=>"Yes",
  #       "corpusCusip"=>"",
  #       "cpiBaseReferencePeriod"=>"",
  #       "currentlyOutstanding"=>"23002000000.000000",
  #       "directBidderAccepted"=>"1026942000",
  #       "directBidderTendered"=>"6778302000",
  #       "estimatedAmountOfPubliclyHeldMaturingSecuritiesByType"=>"114999000000",
  #       "fimaIncluded"=>"Yes",
  #       "fimaNoncompetitiveAccepted"=>"100000000",
  #       "fimaNoncompetitiveTendered"=>"100000000",
  #       "firstInterestPeriod"=>"",
  #       "firstInterestPaymentDate"=>"",
  #       "floatingRate"=>"No",
  #       "frnIndexDeterminationDate"=>"",
  #       "frnIndexDeterminationRate"=>"",
  #       "highDiscountRate"=>"0.030000",
  #       "highInvestmentRate"=>"0.030000",
  #       "highPrice"=>"99.992417",
  #       "highDiscountMargin"=>"",
  #       "highYield"=>"",
  #       "indexRatioOnIssueDate"=>"",
  #       "indirectBidderAccepted"=>"12762957000",
  #       "indirectBidderTendered"=>"13073037000",
  #       "interestPaymentFrequency"=>"None",
  #       "lowDiscountRate"=>"0.010000",
  #       "lowInvestmentRate"=>"",
  #       "lowPrice"=>"",
  #       "lowDiscountMargin"=>"",
  #       "lowYield"=>"",
  #       "maturingDate"=>"2014-08-21T00:00:00",
  #       "maximumCompetitiveAward"=>"10150000000",
  #       "maximumNoncompetitiveAward"=>"5000000",
  #       "maximumSingleBid"=>"10150000000",
  #       "minimumBidAmount"=>"100",
  #       "minimumStripAmount"=>"",
  #       "minimumToIssue"=>"100",
  #       "multiplesToBid"=>"100",
  #       "multiplesToIssue"=>"100",
  #       "nlpExclusionAmount"=>"8100000000",
  #       "nlpReportingThreshold"=>"10150000000",
  #       "noncompetitiveAccepted"=>"454078900",
  #       "noncompetitiveTendersAccepted"=>"Yes",
  #       "offeringAmount"=>"29000000000",
  #       "originalCusip"=>"",
  #       "originalDatedDate"=>"",
  #       "originalIssueDate"=>"2014-05-22T00:00:00",
  #       "originalSecurityTerm"=>"26-Week",
  #       "pdfFilenameAnnouncement"=>"A_20140814_1.pdf",
  #       "pdfFilenameCompetitiveResults"=>"R_20140818_2.pdf",
  #       "pdfFilenameNoncompetitiveResults"=>"NCR_20140818_2.pdf",
  #       "pdfFilenameSpecialAnnouncement"=>"",
  #       "pricePer100"=>"99.992417",
  #       "primaryDealerAccepted"=>"14656180000",
  #       "primaryDealerTendered"=>"115800000000",
  #       "reopening"=>"Yes",
  #       "securityTermDayMonth"=>"91-Day",
  #       "securityTermWeekYear"=>"13-Week",
  #       "series"=>"",
  #       "somaAccepted"=>"0",
  #       "somaHoldings"=>"0",
  #       "somaIncluded"=>"No",
  #       "somaTendered"=>"0",
  #       "spread"=>"",
  #       "standardInterestPaymentPer1000"=>"",
  #       "strippable"=>"",
  #       "term"=>"13-Week",
  #       "tiinConversionFactorPer1000"=>"",
  #       "tips"=>"No",
  #       "totalAccepted"=>"29000157900",
  #       "totalTendered"=>"136205417900",
  #       "treasuryDirectAccepted"=>"294886900",
  #       "treasuryDirectTendersAccepted"=>"Yes",
  #       "type"=>"Bill",
  #       "unadjustedAccruedInterestPer1000"=>"",
  #       "unadjustedPrice"=>"",
  #       "updatedTimestamp"=>"2014-08-18T11:32:48",
  #       "xmlFilenameAnnouncement"=>"A_20140814_1.xml",
  #       "xmlFilenameCompetitiveResults"=>"R_20140818_2.xml",
  #       "xmlFilenameSpecialAnnouncement"=>""
  #     },
  #   ...
  #   ]
  # }
        
  security_list = json["securityList"]
  treasury_bills = security_list.map do |security|
    auction_date = Date.parse(security["auctionDate"])
    issue_date = Date.parse(security["issueDate"])
    maturity_date = Date.parse(security["maturityDate"])
    face_value = BigDecimal.new(100)
    purchase_price = BigDecimal.new(security["pricePer100"])
    high_price = BigDecimal.new(security["highPrice"])
    low_price = BigDecimal.new(security["lowPrice"])
    high_discount_rate = BigDecimal.new(security["highDiscountRate"])
    low_discount_rate = BigDecimal.new(security["lowDiscountRate"])
    maturity_in_days = BigDecimal.new(91)
    TreasuryBill.new(auction_date, issue_date, maturity_date, face_value, purchase_price, high_price, low_price, high_discount_rate, low_discount_rate, maturity_in_days)
  end
  
  lines = ["Date,Value"]
  treasury_bills.each do |tbill|
    auction_date = tbill.auction_date
    # discount_rate = tbill.high_discount_rate / BigDecimal.new(100)    # tbill.high_discount_rate is a percentage, not a fractional value. To convert to a fractional value, divide this by 100
    # computed_discount_rate_from_purchase_price = calculate_discount_rate(tbill.purchase_price, tbill.face_value, tbill.maturity_in_days)    # from 1979-12-28 to 1998-10-26, this calculation differs considerably from computed_discount_rate_from_high_price, but from 1998-11-02 to today, there is no difference; conclusion, use high_price instead of purchase_price in calculation of discount rate
    computed_discount_rate_from_high_price = calculate_discount_rate(tbill.high_price, tbill.face_value, tbill.maturity_in_days)
    
    # puts auction_date
    # puts discount_rate.to_s("F")
    # puts computed_discount_rate_from_purchase_price.to_s("F")
    # puts computed_discount_rate_from_high_price.to_s("F")
    
    date_string = auction_date.strftime("%Y-%m-%d")
    # discount_rate_string = discount_rate.to_s("F")
    # discount_rate_string = computed_discount_rate_from_purchase_price.to_s("F")
    discount_rate_string = computed_discount_rate_from_high_price.to_s("F")
    
    lines << "#{date_string},#{discount_rate_string}"
  end
  File.write(TreasuryBillRateFilePath, lines.join("\n"))
end

# see https://www.treasurydirect.gov/instit/marketables/tbills/tbills.htm
# and http://www.newyorkfed.org/aboutthefed/fedpoint/fed28.html
# for the calculation of the discount rate
# note: both site give the same equation, but the terms are re-arranged. Ultimately, they both lead to:
# d = 360 * (1 - (PP / FV)) / M
# where PP = purchase price, FV = face value, M = maturity in days
One = BigDecimal.new(1)
def calculate_discount_rate(purchase_price, face_value, maturity_in_days)
  BigDecimal.new(360) * (One - (purchase_price / face_value)) / maturity_in_days
end

# create single CSV file containing all futures data
def build_composite_data_table(data_dir)
  file_paths = Dir.glob(File.join(data_dir, "CFE_[A-Z][0-9][0-9]_VX.csv"))
  
  # pp file_paths
  
  # sort file paths in ascending order of year/month - from oldest to most recent
  file_paths = file_paths.sort_by do |path|
    file_name = File.basename(path)
    match = file_name.match(/CFE_([A-Z])([0-9][0-9])_VX.csv/)
    month_code = match[1]
    year = match[2]
    "#{year}/#{month_code}"
  end
  
  all_lines = file_paths.map.with_index do |file_path, i|
    lines = File.readlines(file_path)
    if i == 0
      lines
    else
      lines.drop_while {|line| !(line =~ /^[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}/) }
    end
  end
  
  all_lines = all_lines.flatten.map(&:strip).reject{|line| line.empty? }.join("\n")

  puts 'Saving vix_futures.csv'
  file_name = File.join(DataDir, "vix_futures.csv")
  File.write(file_name, all_lines)
end 

def main
  FileUtils.mkdir_p(DataDir)
  
  puts "Getting 13 Week Treasury Bill Rates"
  save_13_week_treasury_bill_rate
  
  (2004..2015).each do |year|
    (1..12).each do |month|
      puts "Getting data for #{month}/#{year}"
      save_vix_future_data(year, month, DataDir)
    end
  end

  puts "Building composite data table."
  build_composite_data_table(DataDir)
end

main
