require 'pp'
require 'set'
require 'csv'
require 'open-uri'
require 'date'
require 'fileutils'

MonthToMonthCode = (1..12).zip(['F','G','H','J','K','M','N','Q','U','V','X','Z']).to_h    # {1=>"F", 2=>"G", 3=>"H", 4=>"J", 5=>"K", 6=>"M", 7=>"N", 8=>"Q", 9=>"U", 10=>"V", 11=>"X", 12=>"Z"}
MonthCodeToMonth = MonthToMonthCode.invert

TBillDataFile = File.absolute_path(File.join(File.dirname(__FILE__), "data", "tbill13week.csv"))
# tbill13week.csv looks like this:
# Date,Value
# 2014-08-06,0.03
# 2014-08-05,0.03
# ...
# 2005-01-03,2.22

FuturesDataFile = File.absolute_path(File.join(File.dirname(__FILE__), "data", "vix_futures.csv"))
# vix_futures.csv file looks like this:
# Trade Date,Futures,Open,High,Low,Close,Settle,Change,Total Volume,EFP,Open Interest
# 3/26/2004,K (May 04),212.4,212.5,202.7,202.7,203.2,0,216,0,144
# 3/29/2004,K (May 04),199,199.9,197.5,197.7,198,0,52,0,113
# ...
# 07/08/2014,G (Feb 15),16.90,17.14,16.80,16.90,16.90,0.05,1309,0,3298

Today = Date.today

TBillRate = Struct.new(:date, :rate)
Bar = Struct.new(:date, :contract_month, :open, :high, :low, :close, :settle, :change, :volume, :efp, :open_interest)

def build_vix_short_term_index
  last_day_of_vix_futures_data = lookup_bars_by_month(Today.month, Today.year).last.date
  puts "Calculating SPVXSTR from #{CalculateSPVXSTR::BaseDate.to_s} to #{last_day_of_vix_futures_data}. This may take a few minutes."
  CalculateSPVXSTR.build_table(last_day_of_vix_futures_data).each do |date_value_pair|
    date, value = *date_value_pair
    puts "#{date},#{value}"
  end
end

def read_13_week_tbill_rates(file_name = TBillDataFile)
  csv = CSV.table(file_name, :headers => true)
  
  # csv.headers
  # => [:date, :value]
  
  # csv.first
  # => #<CSV::Row date:"2014-08-06" value:0.03>
  
  csv.map do |csv_row|
    TBillRate.new(
      Date.parse(csv_row[:date]),
      csv_row[:value]
    )
  end
end

def tbill_rates_by_date
  @tbill_rates_by_date ||= read_13_week_tbill_rates.reduce({}) {|memo, tbill_rate| memo[tbill_rate.date] = tbill_rate.rate.to_f; memo }
end

def lookup_tbill_rate(date)
  tbill_rates_by_date[date]
end

# returns the tbill rate on the monday at or before <week_end_date>
def most_recent_weekly_high_tbill_rate(week_end_date)
  previous_monday = nth_weekday_at_or_before_date(1, DayOfWeek::Monday, week_end_date)
  date = if cboe_holiday?(previous_monday)
    # On Mondays that are bank holidays, Friday’s rates apply.
    # I believe that means the rates from the Monday prior to last Friday apply, because Treasury announces rates on Monday.
    nth_weekday_before_date(1, DayOfWeek::Monday, previous_monday)
  else
    previous_monday
  end
  
  rate = lookup_tbill_rate(date)
  if rate
    rate
  else
    # puts "#{date} not found, trying #{date - 7}"
    most_recent_weekly_high_tbill_rate(date - 7)
  end
end

# convert a string of the form "mm?/dd?/yyyy" into a Date object
def parse_date(date_string)
  match = date_string.match(/([0-9]{1,2})\/([0-9]{1,2})\/([0-9]{4})/)
  month = match[1].to_i
  day = match[2].to_i
  year = match[3].to_i
  Date.new(year, month, day)
end

# year is an integer of the form yyyy
# month is an integer in [1, 12]
def monthstamp(year, month)
  "#{year}#{month.to_s.rjust(2, "0")}".to_i
end

# convert a string of the form "K (May 04)" into an integer of the form: 200405
def extract_futures_contract_month(futures_contact_string)
  match = futures_contact_string.match(/([FGHJKMNQUVXZ])\s+\([a-zA-Z]{3}\s+(\d{2})\)/)
  contract_month_letter = match[1].upcase
  year_suffix = match[2]
  month = MonthCodeToMonth[contract_month_letter]
  year = "20#{year_suffix}".to_i
  monthstamp(year, month)
end

def read_bars_from_price_history(file_name = FuturesDataFile)
  csv = CSV.table(file_name, :headers => true)
  
  # csv.headers
  # => [:trade_date, :futures, :open, :high, :low, :close, :settle, :change, :total_volume, :efp, :open_interest]

  # csv.first
  # => #<CSV::Row trade_date:"3/26/2004" futures:"K (May 04)" open:212.4 high:212.5 low:202.7 close:202.7 settle:203.2 change:0 total_volume:216 efp:0 open_interest:144>
  
  csv.map do |csv_row|
    Bar.new(
      parse_date(csv_row[:trade_date]),
      extract_futures_contract_month(csv_row[:futures]),
      csv_row[:open].to_f,
      csv_row[:high].to_f,
      csv_row[:low].to_f,
      csv_row[:close].to_f,
      csv_row[:settle].to_f,
      csv_row[:change],
      csv_row[:total_volume],
      csv_row[:efp],
      csv_row[:open_interest]
    )
  end
end

def bars_by_contract_month(file_name = FuturesDataFile)
  @bars_by_contract_month ||= begin
    bars = read_bars_from_price_history(file_name)
    bars_by_contract_month = bars.group_by {|bar| bar.contract_month }
    bars_by_contract_month.each {|contract_month, bars_array| bars_array.sort_by! {|bar| bar.date } }
  end
end

def lookup_bars_by_month(month, year)
  bars_by_contract_month(FuturesDataFile)[monthstamp(year, month)]
end

# ith_month is 1, 2, ...
def lookup_vix_future_eod_bar(ith_month, date)
  raise "ith_month must be >= 1" unless ith_month >= 1
  
  month_offset = if date < start_date_of_roll_period(date.year, date.month)
    # the front month contract is the one expiring at the final settlement date of this month (i.e. date.month)
    ith_month - 1
  else
    # the front month contract is the one expiring at the final settlement date of next month (i.e. next_month(date.month, date.year))
    ith_month
  end
  
  month, year = *add_months(date.month, date.year, month_offset)
  bars = lookup_bars_by_month(month, year)
  bar = bars.binary_search {|bar| date <=> bar.date }
  
  return nil if bar && bar.open == 0 && bar.high == 0 && bar.low == 0 && bar.close == 0 && bar.settle == 0 && bar.volume == 0 && bar.efp == 0 && bar.open_interest == 0
  
  bar
end

# implements algorithm for computing IndexER on page PS-44 of http://app.velocitysharesetns.com/files/prospectus/PRICING_SUPPLEMENT_No__VLS_ETN-1_A31_long_form_2.PDF
# for additional information, see http://www.spindices.com/documents/methodologies/methodology-sp-vix-future-index.pdf
# computes the S&P 500 VIX Short-Term Futures Index ER (ticker: SPVXSP)
class CalculateSPVXSP
  M = 1
  N = 2
  
  def index_er(t)
    raise "#{t.inspect} is not a business day" unless cboe_business_day?(t)
    index_er(prior_cboe_business_day(t)) * (1 + cdr(t))
  end
end

# implements algorithm for computing IndexTR on page PS-21 of http://www.ipathetn.com/static/pdf/vix-prospectus.pdf
# for additional information, see http://www.spindices.com/documents/methodologies/methodology-sp-vix-future-index.pdf
# computes the S&P 500 VIX Short-Term Futures Index TR (ticker: SPVXSTR)
class CalculateSPVXSTR
  M = 1
  N = 2
  BaseDate = Date.new(2005, 12, 20)
  BaseValue = 100000.0
  # BaseDate = Date.new(2009, 1, 29)    # testing against the data linked to by the "IV/Index History" link on http://www.ipathetn.com/us/product/xxv/
  # BaseValue = 159399.380
  
  def self.build_table(end_date = Today)
    calculator = new
    cboe_business_days_between(BaseDate, end_date).map do |date|
      [date, calculator.index_tr(date)]
    end
  end

  def index_tr(t)
    @index_tr ||= {BaseDate => BaseValue}
    @index_tr[t] ||= begin
      raise "#{t.inspect} is not a business day" unless cboe_business_day?(t)
      
      # puts "*" * 80
      # puts "computing index value for #{t}"
      # print_debug_info(t)
      
      index_tr(prior_cboe_business_day(t)) * (1 + cdr(t) + tbr(t))
    end
  end
  
  def print_calendar(year, month, current_date)
    first_day = first_day_of_month(year, month)
    days = days_in_month(year, month)
    last_day = first_day + (days - 1)
    day_of_week_of_first_day = day_of_week(first_day)
    
    settlement_date = vix_futures_settlement_date(year, month)
    
    str = "#{year}/#{month}\n"
    str += " Mon | Tue | Wed | Thu | Fri | Sat | Sun |\n"
    str += " " * ((day_of_week_of_first_day - 1) * 6)
    date_series_inclusive(first_day, last_day).each do |date|
      case
      when cboe_holiday?(date)
        str += "#{date.day}H|".rjust(6, "H")
      when date == current_date
        str += "#{date.day}*|".rjust(6, "*")
      when date == settlement_date
        str += "#{date.day}S|".rjust(6, "S")
      else
        str += "#{date.day} |".rjust(6, " ")
      end
      str += "\n" if day_of_week(date) == DayOfWeek::Sunday
    end
    str
  end
  
  def print_debug_info(t)
    t_minus_1 = prior_cboe_business_day(t)
    previous_month, year_of_previous_month = *previous_month(t.month, t.year)
    subsequent_month1, year_of_subsequent_month1 = *next_month(t.month, t.year)
    subsequent_month2, year_of_subsequent_month2 = *next_month(subsequent_month1, year_of_subsequent_month1)

    puts print_calendar(year_of_previous_month, previous_month, t)
    puts print_calendar(t.year, t.month, t)
    puts print_calendar(year_of_subsequent_month1, subsequent_month1, t)
    puts print_calendar(year_of_subsequent_month2, subsequent_month2, t)
    puts "t = #{t}"
    puts "t_minus_1 = prior_cboe_business_day(#{t}) = #{t_minus_1}"
    puts "vix_futures_settlement_date_for_ith_month_contract(1, #{t}) = #{vix_futures_settlement_date_for_ith_month_contract(1, t)}"
    puts "vix_futures_settlement_date_for_ith_month_contract(2, #{t}) = #{vix_futures_settlement_date_for_ith_month_contract(2, t)}"
    puts "roll_period_for_date(#{t}) = #{roll_period_for_date(t).inspect}"
    puts "lookup_vix_future_eod_bar(1, #{t}) = #{lookup_vix_future_eod_bar(1, t)}"
    puts "lookup_vix_future_eod_bar(2, #{t}) = #{lookup_vix_future_eod_bar(2, t)}"
    puts "dcrp(1, #{t_minus_1}) = #{dcrp(1, t_minus_1)}"
    puts "dcrp(2, #{t_minus_1}) = #{dcrp(2, t_minus_1)}"
    puts "dcrp(1, #{t}) = #{dcrp(1, t)}"
    puts "dcrp(2, #{t}) = #{dcrp(2, t)}"
    puts "dr(#{t_minus_1}) = #{dr(t_minus_1)}"
    puts "dt(#{t_minus_1}) = #{dt(t_minus_1)}"
    puts "dr(#{t}) = #{dr(t)}"
    puts "dt(#{t}) = #{dt(t)}"
    
    index_level = index_tr(t_minus_1) * (1 + cdr(t) + tbr(t))
    puts "index_tr(#{t_minus_1}) * (1 + #{cdr(t)} + #{tbr(t)}) = #{index_level}"
  end

  # Contract Daily Return
  def cdr(t)
    t_minus_1 = prior_cboe_business_day(t)

    # attempt 1 - original equation taken from http://www.spindices.com/documents/methodologies/methodology-sp-vix-future-index.pdf
    # numerator = twdo(t, M, N)
    # divisor = tdwi(t_minus_1, M, N)

    # attempt 2 - just simplified the equation to remove references to twdo and tdwi
    numerator = crw(1, t_minus_1) * dcrp(1, t) + crw(2, t_minus_1) * dcrp(2, t)
    divisor = crw(1, t_minus_1) * dcrp(1, t_minus_1) + crw(2, t_minus_1) * dcrp(2, t_minus_1)

    numerator / divisor - 1.0
  end

  def twdo(t, m, n)
    (m..n).map {|i| crw(i, prior_cboe_business_day(t)) * dcrp(i, t) }.reduce(:+)
  end

  def tdwi(t, m, n)
    (m..n).map {|i| crw(i, t) * dcrp(i, t) }.reduce(:+)
  end
  
  def crw(i, t)
    dt = dt(t)
    dr = dr(t)
    
    weight = if i == 1
      100 * (dr / dt)
    elsif i == 2
      100 * ((dt - dr) / dt)
    end
    
    weight
  end
  
  # i >= 1
  def dcrp(i, t, price_extraction_fn = ->(future_eod_bar) { future_eod_bar.settle })
    ith_bar = lookup_vix_future_eod_bar(i, t)
    if ith_bar
      price_extraction_fn.call(ith_bar)
    else          # ith future not listed
      raise "The first month future is not listed, so we can't interpolate." if i == 1
      
      ith_minus_1_bar = lookup_vix_future_eod_bar(i - 1, t)
      ith_plus_1_bar = lookup_vix_future_eod_bar(i + 1, t)
      ith_plus_2_bar = lookup_vix_future_eod_bar(i + 2, t)
      
      interpolated_value = if ith_plus_1_bar && ith_minus_1_bar      # check to see if ith+1 and ith-1 futures were listed
        dcrp_i_plus_1 = price_extraction_fn.call(ith_plus_1_bar)
        dcrp_i_minus_1 = price_extraction_fn.call(ith_minus_1_bar)
        t_i_minus_1 = vix_futures_settlement_date_for_ith_month_contract(i - 1, t)
        t_i = vix_futures_settlement_date_for_ith_month_contract(i, t)
        t_i_plus_1 = vix_futures_settlement_date_for_ith_month_contract(i + 1, t)
        (dcrp_i_minus_1 ** 2 + (cboe_num_business_days_between(t_i_minus_1, t_i) / cboe_num_business_days_between(t_i_minus_1, t_i_plus_1)) * (dcrp_i_plus_1 ** 2 - dcrp_i_minus_1 ** 2)) ** Rational(1, 2)
      elsif ith_plus_2_bar && ith_minus_1_bar      # check to see if ith+2 and ith-1 futures were listed
        dcrp_i_plus_2 = price_extraction_fn.call(ith_plus_2_bar)
        dcrp_i_minus_1 = price_extraction_fn.call(ith_minus_1_bar)
        t_i_minus_1 = vix_futures_settlement_date_for_ith_month_contract(i - 1, t)
        t_i = vix_futures_settlement_date_for_ith_month_contract(i, t)
        t_i_plus_2 = vix_futures_settlement_date_for_ith_month_contract(i + 2, t)
        (dcrp_i_minus_1 ** 2 + (cboe_num_business_days_between(t_i_minus_1, t_i) / cboe_num_business_days_between(t_i_minus_1, t_i_plus_2)) * (dcrp_i_plus_2 ** 2 - dcrp_i_minus_1 ** 2)) ** Rational(1, 2)
      else
        ith_minus_2_bar = lookup_vix_future_eod_bar(i - 2, t)

        raise "Can't interpolate DCRP" unless ith_minus_1_bar && ith_minus_2_bar
        
        dcrp_i_minus_1 = price_extraction_fn.call(ith_minus_1_bar)
        dcrp_i_minus_2 = price_extraction_fn.call(ith_minus_2_bar)
        t_i_minus_1 = vix_futures_settlement_date_for_ith_month_contract(i - 1, t)
        t_i_minus_2 = vix_futures_settlement_date_for_ith_month_contract(i - 2, t)
        t_i = vix_futures_settlement_date_for_ith_month_contract(i, t)
        (dcrp_i_minus_1 ** 2 + (cboe_num_business_days_between(t_i_minus_1, t_i) / cboe_num_business_days_between(t_i_minus_2, t_i_minus_1)) * (dcrp_i_minus_1 ** 2 - dcrp_i_minus_2 ** 2)) ** Rational(1, 2)
      end
      
      interpolated_value.real.to_f
    end
  end

  def tbr(t)
    previous_business_date = prior_cboe_business_day(t)
    tbar_sub_t_minus_1 = most_recent_weekly_high_tbill_rate(previous_business_date)
    delta_t = (t - previous_business_date).to_i    # the number of calendar days between the current and previous business days
    complex_value = (1.0 / (1 - (91.0 / 360) * tbar_sub_t_minus_1)) ** Rational(delta_t.to_f / 91) - 1    # I don't know why this comes out to a complex number in some cases - I think it's just a Ruby oddity/bug
    complex_value.real
  end
end

##################################################################### S&P VIX Futures Indices date math #####################################################################

# returns the total number of days in the roll period that <date> falls in
#
# dt = The total number of business days in the current Roll Period beginning with
#      and including, the starting CBOE VIX Futures Settlement Date and ending
#      with, but excluding, the following CBOE VIX Futures Settlement Date. The
#      number of business days stays constant in cases of a new holiday introduced
#      intra-month or an unscheduled market closure.
def dt(date)
  start_date, end_date = *roll_period_for_date(date)
  start_date = start_date + 1   # the start date of the roll period is at the close of the tuesday prior to the settlement date, so we add 1 to bump up the start date to the wednesday settlement date
  business_days_in_roll_period = date_series_inclusive(start_date, end_date).select {|date| cboe_business_day?(date, false) }
  business_days_in_roll_period.count.to_f
end

# returns the number of days remaining in the roll period that <date> falls in
#
# dr = The total number of business days within a roll period beginning with, and
#      including the following business day and ending with, but excluding, the following
#      CBOE VIX Futures Settlement Date. The number of business days includes a
#      new holiday introduced intra-month up to the business day preceding such a holiday.
def dr(date)
  _, end_date = *roll_period_for_date(date)
  start_date = next_cboe_business_day(date)
  remaining_business_days_in_roll_period = date_series_inclusive(start_date, end_date).select {|date| cboe_business_day?(date) }
  remaining_business_days_in_roll_period.count.to_f
end

# returns 2 dates, [start_date_of_roll_period, end_date_of_roll_period], representing the start date and end date of the VIX futures roll period
# The roll period begins at the market closes on the start_date_of_roll_period and ends at (but excludes) the market close on the end_date_of_roll_period.
# In other words, the roll period is [market close on start date, market close on end date)
# Defined on page 7 of http://www.spindices.com/documents/methodologies/methodology-sp-vix-future-index.pdf
# The Roll Period starts on the Tuesday prior to the monthly CBOE VIX/VXEEM Futures Settlement Date (the Wednesday falling 30 calendar days before the S&P 500 option 
# expiration for the following month), and runs through the Tuesday prior to the subsequent month’s CBOE VIX/VXEEM Futures Settlement Date.
# ...
# At the close on the Tuesday, corresponding to the start of the Roll Period, all of the weight is allocated to the shorter-term (i.e. mth month) contract. ...
def roll_period_starting_with_month(month, year)
  subsequent_month, year_of_subsequent_month = *next_month(month, year)
  start_date = start_date_of_roll_period(year, month)
  end_date = start_date_of_roll_period(year_of_subsequent_month, subsequent_month)
  [start_date, end_date]
end

# It is assumed that the observation given by <date> is observed at the market close on <date>
def roll_period_for_date(date)
  if date < start_date_of_roll_period(date.year, date.month)
    previous_month, year_of_previous_month = *previous_month(date.month, date.year)
    roll_period_starting_with_month(previous_month, year_of_previous_month)
  else
    roll_period_starting_with_month(date.month, date.year)
  end
end

# Defined on page 7 of http://www.spindices.com/documents/methodologies/methodology-sp-vix-future-index.pdf
# The Roll Period starts on the Tuesday prior to the monthly CBOE VIX/VXEEM Futures Settlement Date
# Note: This doesn't say anything about whether the market was open. It just says the Tuesday before the settlement date.
def start_date_of_roll_period(year, month)
  # todo, to be truly accurate with respect to the description, this should figure out the date of the Tuesday prior to the settlement date, but I think subtracting one day is the real intention.
  vix_futures_settlement_date(year, month) - 1
  # prior_cboe_business_day(vix_futures_settlement_date(year, month))
end

# vix_futures_settlement_date(year, month) = the Wednesday falling 30 calendar days before the S&P 500 option expiration for the following month (i.e. month + 1)
# The final settlement date is also the expiration date.
# from http://cfe.cboe.com/products/spec_vix.aspx:
# The Wednesday that is thirty days prior to the third Friday of the calendar month immediately following the
# month in which the contract expires ("Final Settlement Date"). If the third Friday of the month
# subsequent to expiration of the applicable VIX futures contract is a CBOE holiday, the Final Settlement Date
# for the contract shall be thirty days prior to the CBOE business day immediately preceding that Friday.
def vix_futures_settlement_date(year, month)
  subsequent_month, year_of_subsequent_month = *next_month(month, year)
  third_friday_of_subsequent_month = nth_weekday_of_month(3, DayOfWeek::Friday, subsequent_month, year_of_subsequent_month)
  if cboe_holiday?(third_friday_of_subsequent_month)
    prior_cboe_business_day(third_friday_of_subsequent_month) - 30
  else
    third_friday_of_subsequent_month - 30
  end
end

def vix_futures_settlement_date_for_ith_month_contract(ith_month, date)
  raise "ith_month may not be zero" if ith_month == 0
  
  month_offset = if ith_month > 0
    if date < vix_futures_settlement_date(date.year, date.month)
      # the front month contract is the one expiring at the final settlement date of this month (i.e. date.month)
      ith_month - 1
    else
      # the front month contract is the one expiring at the final settlement date of next month (i.e. next_month(date.month, date.year))
      ith_month
    end
  elsif ith_month < 0
    if date > vix_futures_settlement_date(date.year, date.month)
      # the "front month" (looking backward in time) contract is the one expiring at the final settlement date of this month (i.e. date.month)
      ith_month + 1
    else
      # the "front month" (looking backward in time) contract is the one expiring at the final settlement date of the previous month (i.e. previous_month(date.month, date.year))
      ith_month
    end
  end
  
  month, year = *add_months(date.month, date.year, month_offset)
  
  vix_futures_settlement_date(year, month)
end

def cboe_business_days_between(start_date, end_date, include_end_date = false)
  if include_end_date
    date_series_inclusive(start_date, end_date).select {|date| cboe_business_day?(date) }
  else
    date_series(start_date, end_date).select {|date| cboe_business_day?(date) }
  end
end

def cboe_num_business_days_between(start_date, end_date, include_end_date = false)
  cboe_business_days_between(start_date, end_date, include_end_date).count
end

def prior_cboe_business_day(date)
  previous_date = prior_business_day(date)
  if cboe_holiday?(previous_date)
    prior_cboe_business_day(previous_date)
  else
    previous_date
  end
end

def next_cboe_business_day(date)
  next_date = next_business_day(date)
  if cboe_holiday?(next_date)
    next_cboe_business_day(next_date)
  else
    next_date
  end
end

def cboe_business_day?(date, include_unscheduled_market_closures = true)
  business_day?(date) && !cboe_holiday?(date, include_unscheduled_market_closures)
end

# sources:
# http://www1.nyse.com/pdfs/closings.pdf
# http://www.cboe.com/publish/RegCir/RG12-150.pdf
UnscheduledMarketClosures = [
  Date.new(2007, 1, 2),     # Closed in observance of the National Day of Mourning for former President Gerald R. Ford (died December 26, 2006).
  Date.new(2012, 10, 29),   # Closed Monday for Hurricane Sandy
  Date.new(2012, 10, 30)    # Closed Tuesday for Hurricane Sandy
].to_set

# see the holiday rules at: http://cfe.cboe.com/aboutcfe/ExpirationCalendar.aspx
def cboe_holiday?(date, include_unscheduled_market_closures = true)
  today_is_unscheduled_closure = include_unscheduled_market_closures ? UnscheduledMarketClosures.include?(date) : false
  today_is_friday_and_saturday_is_holiday = day_of_week(date) == DayOfWeek::Friday && 
                                            (good_friday?(date + 1) || independence_day?(date + 1) || christmas?(date + 1))
  today_is_monday_and_sunday_is_holiday = day_of_week(date) == DayOfWeek::Monday && holiday?(date - 1)
  holiday?(date) || today_is_friday_and_saturday_is_holiday || today_is_monday_and_sunday_is_holiday || today_is_unscheduled_closure
end


##################################################################### date math #####################################################################

def date_series(start_date, end_date, incrementer_fn = ->(date){ date + 1 })
  series = []
  date = start_date
  while date < end_date
    series << date
    date = incrementer_fn.call(date)
  end
  series
end

def date_series_inclusive(start_date, end_date, incrementer_fn = ->(date){ date + 1 })
  series = []
  date = start_date
  while date <= end_date
    series << date
    date = incrementer_fn.call(date)
  end
  series
end

def prior_business_day(date)
  if day_of_week(date) == DayOfWeek::Monday
    date - 3
  else
    date - 1
  end
end

def next_business_day(date)
  if day_of_week(date) == DayOfWeek::Friday
    date + 3
  else
    date + 1
  end
end

def business_day?(date)
  day_of_week(date) < DayOfWeek::Saturday    # is date Mon/Tue/Wed/Thu/Fri ?
end

# returns [month, year] representing the month and year following the given month and year
def next_month(month, year)
  if month == 12
    [1, year + 1]
  else
    [month + 1, year]
  end
end

# returns [month, year] representing the month and year preceeding the given month and year
def previous_month(month, year)
  if month == 1
    [12, year - 1]
  else
    [month - 1, year]
  end
end

def add_months(base_month, base_year, month_offset)
  if month_offset >= 0
    month_offset.times.reduce([base_month, base_year]) do |memo, i|
      month, year = *memo
      next_month(month, year)
    end
  else
    (-month_offset).times.reduce([base_month, base_year]) do |memo, i|
      month, year = *memo
      previous_month(month, year)
    end
  end
end

def first_day_of_month(year, month)
  Date.new(year, month, 1)
end

COMMON_YEAR_DAYS_IN_MONTH = [nil, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
def days_in_month(year, month)
  if month == 2 && ::Date.gregorian_leap?(year)
    29
  else
    COMMON_YEAR_DAYS_IN_MONTH[month]
  end
end

# returns the day of calendar week (1-7, Monday is 1).
def day_of_week(date)
  date.cwday    # cwday returns the day of calendar week (1-7, Monday is 1).
end

# Returns the number of days that must be added to the first day of the given month to arrive at the first
#   occurrence of the <desired-weekday> in that month; put another way, it returns the number of days
#   that must be added to the first day of the given month to arrive at the <desired-weekday> in the first
#   week of that month.
# The return value will be an integer in the range [0, 6].
# NOTE: the return value is the result of the following expression:
#   (desired-weekday - dayOfWeek(year, month, 1) + 7) mod 7
# desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# month is an integer indicating the month, s.t. 1=Jan., 2=Feb., ..., 11=Nov., 12=Dec.
# year is an integer indicating the year (e.g. 1999, 2010, 2012, etc.)
# Example:
#   offset_of_first_weekday_in_month(1, 2, 2012)    ; monday
#   > 5
#   offset_of_first_weekday_in_month(3, 2, 2012)    ; wednesday
#   > 0
#   offset_of_first_weekday_in_month(5, 2, 2012)    ; friday
#   > 2
def offset_of_first_weekday_in_month(desired_weekday, month, year)
  offset_of_first_weekday_at_or_after_weekday(desired_weekday, day_of_week(first_day_of_month(year, month)))
end

# The return value will be an integer in the range [1, 7].
# desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# offset_of_first_weekday_after_weekday(2, 2) => 7
# offset_of_first_weekday_after_weekday(5, 2) => 3
# offset_of_first_weekday_after_weekday(3, 6) => 4
def offset_of_first_weekday_after_weekday(desired_weekday, current_weekday)
  offset = offset_of_first_weekday_at_or_after_weekday(desired_weekday, current_weekday)
  offset == 0 ? 7 : offset
end

# The return value will be an integer in the range [-7, -1].
# desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# offset_of_first_weekday_before_weekday(2, 2) => -7
# offset_of_first_weekday_before_weekday(5, 2) => -4
# offset_of_first_weekday_before_weekday(3, 6) => -3
def offset_of_first_weekday_before_weekday(desired_weekday, current_weekday)
  offset = offset_of_first_weekday_at_or_before_weekday(desired_weekday, current_weekday)
  offset == 0 ? -7 : offset
end

# The return value will be an integer in the range [0, 6].
# desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
def offset_of_first_weekday_at_or_after_weekday(desired_weekday, current_weekday)
  (desired_weekday - current_weekday + 7) % 7
end

# The return value will be an integer in the range [-6, 0].
# desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# current_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
def offset_of_first_weekday_at_or_before_weekday(desired_weekday, current_weekday)
  -((current_weekday - desired_weekday + 7) % 7)
end

# desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# first_weekday_after_date(DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
# first_weekday_after_date(DayOfWeek::Friday, Date.new(2012, 2, 24)) => #<Date: 2012-03-02 ((2455989j,0s,0n),+0s,2299161j)>
def first_weekday_after_date(desired_weekday, date)
  offset = offset_of_first_weekday_after_weekday(desired_weekday, day_of_week(date))
  date + offset
end

# desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# first_weekday_at_or_after_date(DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
# first_weekday_at_or_after_date(DayOfWeek::Friday, Date.new(2012, 2, 24)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
def first_weekday_at_or_after_date(desired_weekday, date)
  offset = offset_of_first_weekday_at_or_after_weekday(desired_weekday, day_of_week(date))
  date + offset
end

# desired_weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# first_weekday_before_date(DayOfWeek::Friday, Date.new(2012, 3, 2)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
# first_weekday_before_date(DayOfWeek::Wednesday, Date.new(2012, 3, 2)) => #<Date: 2012-02-29 ((2455987j,0s,0n),+0s,2299161j)>
def first_weekday_before_date(desired_weekday, date)
  offset = offset_of_first_weekday_before_weekday(desired_weekday, day_of_week(date))
  date + offset
end

def first_weekday_at_or_before_date(desired_weekday, date)
  offset = offset_of_first_weekday_at_or_before_weekday(desired_weekday, day_of_week(date))
  date + offset
end

module DayOfWeek
  Monday = 1
  Tuesday = 2
  Wednesday = 3
  Thursday = 4
  Friday = 5
  Saturday = 6
  Sunday = 7
end

module Month
  January = 1
  February = 2
  March = 3
  April = 4
  May = 5
  June = 6
  July = 7
  August = 8
  September = 9
  October = 10
  November = 11
  December = 12
end

# returns a LocalDate representing the nth weekday in the given month.
# desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# month is an integer indicating the month, s.t. 1=Jan., 2=Feb., ..., 11=Nov., 12=Dec.
# year is an integer indicating the year (e.g. 1999, 2010, 2012, etc.)
# Example:
#   nth_weekday_of_month(3, DayOfWeek::Monday, 1, 2012)    ; returns the 3rd monday in January 2012.
#   => #<Date: 2012-01-16 ((2455943j,0s,0n),+0s,2299161j)>
#   nth_weekday_of_month(3, DayOfWeek::Monday, 2, 2012)    ; returns the 3rd monday in February 2012.
#   => #<Date: 2012-02-20 ((2455978j,0s,0n),+0s,2299161j)>
#   nth_weekday_of_month(1, DayOfWeek::Wednesday, 2, 2012) ; returns the 1st wednesday in February 2012.
#   => #<Date: 2012-02-01 ((2455959j,0s,0n),+0s,2299161j)>
def nth_weekday_of_month(n, desired_weekday, month, year)
  nth_weekday_at_or_after_date(n, desired_weekday, first_day_of_month(year, month))
end

# returns a Date representing the nth weekday after the given date
# desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# nth_weekday_after_date(1, DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
# nth_weekday_after_date(2, DayOfWeek::Friday, Date.new(2012, 2, 18)) => #<Date: 2012-03-02 ((2455989j,0s,0n),+0s,2299161j)>
# nth_weekday_after_date(4, DayOfWeek::Wednesday, Date.new(2012, 2, 18)) => #<Date: 2012-03-14 ((2456001j,0s,0n),+0s,2299161j)>
def nth_weekday_after_date(n, desired_weekday, date)
  week_offset_in_days = 7 * (n - 1)
  first_weekday_after_date(desired_weekday, date) + week_offset_in_days
end

# returns a Date representing the nth weekday after the given date
# desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# nth_weekday_at_or_after_date(1, DayOfWeek::Friday, Date.new(2012, 2, 3)) => #<Date: 2012-02-03 ((2455961j,0s,0n),+0s,2299161j)>
# nth_weekday_at_or_after_date(2, DayOfWeek::Friday, Date.new(2012, 2, 3)) => #<Date: 2012-02-10 ((2455968j,0s,0n),+0s,2299161j)>
def nth_weekday_at_or_after_date(n, desired_weekday, date)
  week_offset_in_days = 7 * (n - 1)
  first_weekday_at_or_after_date(desired_weekday, date) + week_offset_in_days
end

# returns a Date representing the nth weekday after the given date
# desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# Example:
# nth_weekday_before_date(1, DayOfWeek::Friday, Date.new(2012, 3, 2)) => #<Date: 2012-02-24 ((2455982j,0s,0n),+0s,2299161j)>
# nth_weekday_before_date(2, DayOfWeek::Friday, Date.new(2012, 3, 2)) => #<Date: 2012-02-17 ((2455975j,0s,0n),+0s,2299161j)>
# nth_weekday_before_date(4, DayOfWeek::Wednesday, Date.new(2012, 3, 2)) => #<Date: 2012-02-08 ((2455966j,0s,0n),+0s,2299161j)>
def nth_weekday_before_date(n, desired_weekday, date)
  week_offset_in_days = 7 * (n - 1)
  first_weekday_before_date(desired_weekday, date) - week_offset_in_days
end

def nth_weekday_at_or_before_date(n, desired_weekday, date)
  week_offset_in_days = 7 * (n - 1)
  first_weekday_at_or_before_date(desired_weekday, date) - week_offset_in_days
end

# Returns a LocalDate representing the last weekday in the given month.
# desired-weekday is an integer indicating the desired day of the week, s.t. 1=Monday, 2=Tue., ..., 6=Sat., 7=Sun.
# month is an integer indicating the month, s.t. 1=Jan., 2=Feb., ..., 11=Nov., 12=Dec.
# year is an integer indicating the year (e.g. 1999, 2010, 2012, etc.)
# source: http://www.irt.org/articles/js050/
# formula:
#   daysInMonth - (DayOfWeek(daysInMonth,month,year) - desiredWeekday + 7)%7
# Example:
#   last_weekday(DayOfWeek::Monday, 2, 2012)
#   => #<Date: 2012-02-27 ((2455985j,0s,0n),+0s,2299161j)>
def last_weekday(desired_weekday, month, year)
  days = days_in_month(year, month)
  day_of_month = days - (day_of_week(Date.new(year, month, days)) - desired_weekday + 7) % 7
  Date.new(year, month, day_of_month)
end

##################################################################### special dates #####################################################################

def new_years(year)
  Date.new(year, 1, 1)
end

def new_years?(date)
  new_years(date.year) == date
end

def martin_luther_king_jr_day(year)
  nth_weekday_of_month(3, DayOfWeek::Monday, Month::January, year)
end

def martin_luther_king_jr_day?(date)
  martin_luther_king_jr_day(date.year) == date
end

def presidents_day(year)
  nth_weekday_of_month(3, DayOfWeek::Monday, Month::February, year)
end

def presidents_day?(date)
  presidents_day(date.year) == date
end

def memorial_day(year)
  last_weekday(DayOfWeek::Monday, Month::May, year)
end

def memorial_day?(date)
  memorial_day(date.year) == date
end

def independence_day(year)
  Date.new(year, Month::July, 4)
end

def independence_day?(date)
  independence_day(date.year) == date
end

def labor_day(year)
  nth_weekday_of_month(1, DayOfWeek::Monday, Month::September, year)
end

def labor_day?(date)
  labor_day(date.year) == date
end

def columbus_day(year)
  nth_weekday_of_month(2, DayOfWeek::Monday, Month::October, year)
end

def columbus_day?(date)
  columbus_day(date.year) == date
end

def thanksgiving(year)
  nth_weekday_of_month(4, DayOfWeek::Thursday, Month::November, year)
end

def thanksgiving?(date)
  thanksgiving(date.year) == date
end

def christmas(year)
  Date.new(year, Month::December, 25)
end

def christmas?(date)
  christmas(date.year) == date
end

# This is a non-trivial calculation. See http://en.wikipedia.org/wiki/Computus
#   "Computus (Latin for "computation") is the calculation of the date of Easter in the Christian calendar."
#   Evidently the scientific study of computation (or Computer Science, as we like to call it) was born out
#   of a need to calculate when Easter was going to be.
# See http://www.linuxtopia.org/online_books/programming_books/python_programming/python_ch38.html
# The following code was taken from: http://www.merlyn.demon.co.uk/estralgs.txt
# function McClendon(YR) {
#   var g, c, x, z, d, e, n
#   g = YR % 19 + 1   // Golden
#   c = ((YR/100)|0) + 1    // Century
#   x = ((3*c/4)|0) - 12    // Solar
#   z = (((8*c+5)/25)|0) - 5  // Lunar
#   d = ((5*YR/4)|0) - x - 10 // Letter ?
#   e = (11*g + 20 + z - x) % 30  // Epact
#   if (e<0) e += 30    // Fix 9006 problem
#   if ( ( (e==25) && (g>11) ) || (e==24) ) e++
#   n = 44 - e
#   if (n<21) n += 30   // PFM
#   return n + 7 - ((d+n)%7)  // Following Sunday
#   }
def easter(year)
  g = year % 19 + 1
  c = year / 100 + 1
  x = (3 * c / 4) - 12
  z = (8 * c + 5) / 25 - 5
  d = 5 * year / 4 - x - 10
  e = (11 * g + 20 + z - x) % 30
  e1 = e < 0 ? e + 30 : e
  e2 = (e1 == 25 && g > 11) || e1 == 24 ? e1 + 1 : e1
  n = 44 - e2
  n1 = n < 21 ? n + 30 : n
  n2 = (n1 + 7) - ((d + n1) % 7)
  day = n2 > 31 ? n2 - 31 : n2
  month = n2 > 31 ? 4 : 3
  Date.new(year, month, day)
end

def easter?(date)
  easter(date.year) == date
end

def good_friday(year)
  easter(year) - 2
end

def good_friday?(date)
  good_friday(date.year) == date
end

HolidayLookupFunctions = [
  ->(date) { new_years?(date) },
  ->(date) { martin_luther_king_jr_day?(date) },
  ->(date) { presidents_day?(date) },
  ->(date) { good_friday?(date) },
  ->(date) { memorial_day?(date) },
  ->(date) { independence_day?(date) },
  ->(date) { labor_day?(date) },
  ->(date) { columbus_day?(date) },
  ->(date) { thanksgiving?(date) },
  ->(date) { christmas?(date) }
]
def holiday?(date)
  HolidayLookupFunctions.any? {|holiday_fn| holiday_fn.call(date) }
end


# taken from https://github.com/tyler/binary_search/blob/master/lib/binary_search/pure.rb
class Array
  def binary_index(target, &comparator)
    if comparator
      binary_chop(&comparator)
    else
      binary_chop {|v| target <=> v }
    end
  end

  def binary_search(&comparator)
    index = binary_chop(&comparator)
    index ? self[index] : nil
  end

  private

  def binary_chop(&comparator)
    upper = self.size - 1
    lower = 0

    while(upper >= lower) do
      idx = lower + (upper - lower) / 2
      comp = comparator.call(self[idx])

      if comp == 0
        return idx
      elsif comp > 0
        lower = idx + 1
      else
        upper = idx - 1
      end
    end
    nil
  end
end


def main
  build_vix_short_term_index
end

main
