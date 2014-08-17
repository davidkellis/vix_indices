require_relative 'yahoofinance'

require 'pp'
require 'csv'
require 'nokogiri'
require 'open-uri'
require 'date'
require 'fileutils'
 
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

def save_13_week_treasury_bill_rate
  start_date = Date.new(2005, 1, 1)
  end_date = Today
  lines = ["Date,Value"]
  YahooFinance.get_historical_quotes("^IRX", start_date, end_date) do |row|
    # each row is of the form: [date (yyyy-mm-dd), open, high, low, close, volume, adj-close]
    date, open, high, low, close, volume, adj_close = *row
    lines << "#{date},#{high.to_f / 100.0}"
  end
  File.write(TreasuryBillRateFilePath, lines.join("\n"))
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
