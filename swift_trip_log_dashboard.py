import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Page configuration
st.set_page_config(
    page_title="Swift Trip Log Dashboard",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Auto-refresh every 10 minutes (600000 milliseconds)
components.html(
    """
    <script>
        setTimeout(function(){
            window.parent.location.reload();
        }, 600000);
    </script>
    """,
    height=0
)

# Custom CSS
st.markdown("""
<style>
    .metric-card-blue {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-card-red {
        background: linear-gradient(135deg, #8b2635 0%, #c73e4d 100%);
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #b0b0b0;
        margin-bottom: 0.5rem;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #ffffff;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: rgba(28, 131, 225, 0.1);
        border-radius: 4px;
        padding: 8px 16px;
        color: #FFFFFF;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(28, 131, 225, 0.4);
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_triplog_data():
    """Load trip log data from JSON file"""
    try:
        with open('/Users/swiftroadlink/Documents/DE/triplog_api_data.json', 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Error loading triplog data: {e}")
        return pd.DataFrame()


def load_targets():
    """Load target SQR data from JSON file"""
    try:
        with open('/Users/swiftroadlink/Documents/Dashboard/Swift trip log/sob_targets.json', 'r') as f:
            targets = json.load(f)
        return targets
    except:
        return {}


def save_targets(targets):
    """Save target SQR data to JSON file"""
    try:
        with open('/Users/swiftroadlink/Documents/Dashboard/Swift trip log/sob_targets.json', 'w') as f:
            json.dump(targets, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving targets: {e}")
        return False


def get_client_category(party_name):
    """Categorize clients into groups based on NewPartyName"""
    if pd.isna(party_name) or party_name == "":
        return "Other"
    party_upper = str(party_name).upper()
    if "HONDA" in party_upper or "TAPUKERA" in party_upper:
        return "Honda"
    elif "MAHINDRA" in party_upper or "M & M" in party_upper or "M&M" in party_upper or "MSTC" in party_upper or "TRAIN LOAD" in party_upper:
        return "M & M"
    elif "TOYOTA" in party_upper or "TRANSYSTEM" in party_upper or "DC MOVEMENT" in party_upper:
        return "Toyota"
    elif "SKODA" in party_upper or "VOLKSWAGEN" in party_upper:
        return "Skoda"
    elif "GLOVIS" in party_upper:
        return "Glovis"
    elif "TATA" in party_upper:
        return "Tata"
    elif "MARKET LOAD" in party_upper:
        return "Market Load"
    else:
        return "Other"


def main():
    # Header
    st.markdown("<h1 style='text-align: center;'>🚛 Swift Trip Log Dashboard</h1>", unsafe_allow_html=True)

    # Load data
    with st.spinner("Loading data..."):
        df = load_triplog_data()
        targets = load_targets()

    if df.empty:
        st.error("No data available. Please check the JSON files.")
        return

    # Convert date columns
    df['LoadingDate'] = pd.to_datetime(df['LoadingDate'], errors='coerce')
    df['CarQty'] = pd.to_numeric(df['CarQty'], errors='coerce').fillna(0)
    df['Freight'] = pd.to_numeric(df['Freight'], errors='coerce').fillna(0)
    df['LRFreight'] = pd.to_numeric(df['LRFreight'], errors='coerce').fillna(0)
    df['Distance'] = pd.to_numeric(df['Distance'], errors='coerce').fillna(0)

    # Use NewPartyName if available, otherwise fall back to Party
    if 'NewPartyName' in df.columns:
        df['DisplayParty'] = df['NewPartyName'].fillna(df['Party'])
    else:
        df['DisplayParty'] = df['Party']

    # Add client category based on DisplayParty
    df['category'] = df['DisplayParty'].apply(get_client_category)

    # Sidebar Filters
    st.sidebar.header("Filters")

    # Select Month
    st.sidebar.subheader("Select Month")
    available_months = df['LoadingDate'].dt.to_period('M').dropna().unique()
    available_months = sorted([str(m) for m in available_months], reverse=True)

    if available_months:
        month_options = {pd.to_datetime(m).strftime("%b'%y"): m for m in available_months}
        selected_month_display = st.sidebar.selectbox("Month", list(month_options.keys()))
        selected_month_str = month_options[selected_month_display]
        selected_month = pd.to_datetime(selected_month_str)
    else:
        selected_month = datetime.now()

    # Filter by Party
    st.sidebar.subheader("Filter by Party")
    all_parties = ['All'] + sorted(df['DisplayParty'].dropna().unique().tolist())
    selected_party = st.sidebar.selectbox("Party", all_parties)

    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    # Target SOB Update Section
    st.sidebar.markdown("---")
    st.sidebar.subheader("Update Target SOB")

    with st.sidebar.expander("Set Target SOB", expanded=False):
        # Get unique parties for target setting
        unique_parties = sorted(df['DisplayParty'].dropna().unique().tolist())
        unique_parties = [p for p in unique_parties if p != '']

        # Select party to update
        target_party = st.selectbox("Select Party", [''] + unique_parties, key='target_party')

        if target_party:
            current_target = targets.get(target_party, 0)
            new_target = st.number_input("Target SOB", min_value=0, value=int(current_target), key='new_target')

            if st.button("💾 Save Target"):
                targets[target_party] = new_target
                if save_targets(targets):
                    st.success(f"Target saved for {target_party}: {new_target}")
                    st.rerun()

        # Show current targets
        st.markdown("**Current Targets:**")
        if targets:
            for party, target in sorted(targets.items()):
                st.text(f"{party[:20]}: {target}")
        else:
            st.text("No targets set")

    # Filter data for selected month
    month_start = selected_month.replace(day=1)
    month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)

    # Convert to date only for accurate comparison
    df['LoadingDateOnly'] = df['LoadingDate'].dt.date

    # Full month data for summary boxes (all available data for the month)
    month_df = df[(df['LoadingDateOnly'] >= month_start.date()) & (df['LoadingDateOnly'] <= month_end.date())]

    if selected_party != 'All':
        month_df = month_df[month_df['DisplayParty'] == selected_party]

    # Month Summary Section (Full Month - Live Data)
    st.markdown(f"### Month Summary ({selected_month.strftime('%B %Y')})")

    # Calculate metrics
    loaded_trips = len(month_df[(month_df['TripStatus'] != 'Empty') & (month_df['DisplayParty'] != '') & (month_df['DisplayParty'].notna())])
    empty_trips = len(month_df[(month_df['TripStatus'] == 'Empty') | (month_df['DisplayParty'] == '') | (month_df['DisplayParty'].isna())])
    cars_lifted = int(month_df['CarQty'].sum())
    total_freight = month_df['Freight'].sum()
    total_freight_lakhs = total_freight / 100000

    # Display metrics in cards
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="metric-card-blue">
            <div class="metric-label">Loaded Trips</div>
            <div class="metric-value">{loaded_trips:,}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card-blue">
            <div class="metric-label">Empty Trips</div>
            <div class="metric-value">{empty_trips:,}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="metric-card-blue">
            <div class="metric-label">Cars Lifted</div>
            <div class="metric-value">{cars_lifted:,}</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="metric-card-red">
            <div class="metric-label">Total Freight</div>
            <div class="metric-value">₹{total_freight_lakhs:.2f}L</div>
        </div>
        """, unsafe_allow_html=True)

    # Initialize session state for tab selection
    if 'selected_tab' not in st.session_state:
        st.session_state.selected_tab = 0

    # Tabs with key to maintain state
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Target vs Actual", "📅 Daily Loading Details", "🚚 Local/Pilot Loads", "🗺️ Zone View"])

    with tab1:
        month_display = selected_month.strftime("%b'%y")
        st.markdown(f"**Selected Month:** {month_display}")

        # Target vs Actual - Client-Wise Summary
        st.markdown("### Target vs Actual - Client-Wise Summary")

        # Date selectors
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            select_month_start = st.date_input("Select Month", month_start.date())
        with col2:
            # Default to D-1 (yesterday)
            yesterday = (datetime.now() - timedelta(days=1)).date()
            till_date = st.date_input("Till Date", min(yesterday, month_end.date()))
        with col3:
            compare_month = st.date_input("Compare With Month", (month_start - relativedelta(months=2)).date())
        with col4:
            compare_till_date = st.date_input("Compare Till Date", (pd.to_datetime(compare_month) + timedelta(days=till_date.day - 1)).date())

        # Filter data for current period (using date only for accurate comparison)
        current_df = df[
            (df['LoadingDateOnly'] >= select_month_start) &
            (df['LoadingDateOnly'] <= till_date) &
            (df['DisplayParty'] != '') &
            (df['DisplayParty'].notna())
        ]

        # Filter data for comparison period
        compare_df = df[
            (df['LoadingDateOnly'] >= compare_month) &
            (df['LoadingDateOnly'] <= compare_till_date) &
            (df['DisplayParty'] != '') &
            (df['DisplayParty'].notna())
        ]

        till_date_display = till_date.strftime("%dth %b'%y")
        st.markdown(f"#### Till {till_date_display} 📊")

        # Create client-wise summary
        summary = current_df.groupby('DisplayParty').agg({
            'TLHSNo': 'count',
            'CarQty': 'sum',
            'Freight': 'sum'
        }).reset_index()
        summary.columns = ['Party', 'Trips', 'Total_Cars', 'Total_Freight']
        summary['category'] = summary['Party'].apply(get_client_category)

        # All are "Own" for now
        summary['Own_Cars'] = summary['Total_Cars']
        summary['Vendor_Cars'] = 0
        summary['Own_Freight'] = summary['Total_Freight']
        summary['Vendor_Freight'] = 0

        # Add comparison data
        if not compare_df.empty:
            compare_summary = compare_df.groupby('DisplayParty').agg({
                'CarQty': 'sum',
                'Freight': 'sum'
            }).reset_index()
            compare_summary.columns = ['Party', 'Compare_Cars', 'Compare_Freight']
            summary = summary.merge(compare_summary, on='Party', how='left')
            summary['Compare_Cars'] = summary['Compare_Cars'].fillna(0)
            summary['Compare_Freight'] = summary['Compare_Freight'].fillna(0)
        else:
            summary['Compare_Cars'] = 0
            summary['Compare_Freight'] = 0

        # Build final table with category totals
        category_order = ['Honda', 'M & M', 'Toyota', 'Skoda', 'Glovis', 'Tata', 'Market Load', 'Other']
        category_colors = {
            'Honda': '#ff6b35',
            'M & M': '#2e8b57',
            'Toyota': '#4169e1',
            'Skoda': '#8b5cf6',
            'Glovis': '#f59e0b',
            'Tata': '#06b6d4',
            'Market Load': '#ec4899',
            'Other': '#6b7280',
            'Grand': '#1e40af'
        }

        final_rows = []
        grand_total = {'Own': 0, 'Vendor': 0, 'Total': 0, 'Own_F': 0, 'Vendor_F': 0, 'Total_F': 0, 'Cars_Comp': 0, 'Freight_Comp': 0, 'Target_SQR': 0}

        for category in category_order:
            cat_df = summary[summary['category'] == category].copy()
            if len(cat_df) > 0:
                # Calculate category target sum from individual rows
                category_target_sum = 0

                # Add individual rows
                for _, row in cat_df.iterrows():
                    target_sqr = targets.get(row['Party'], 0)
                    if target_sqr:
                        category_target_sum += target_sqr
                    final_rows.append({
                        'Client - Wise': row['Party'],
                        'Target SQR': target_sqr if target_sqr else '',
                        'Own': int(row['Own_Cars']),
                        'Vendor': int(row['Vendor_Cars']),
                        'Total': int(row['Total_Cars']),
                        'Own_F': row['Own_Freight'],
                        'Vendor_F': row['Vendor_Freight'],
                        'Total_F': row['Total_Freight'],
                        'Cars_Comp': int(row['Compare_Cars']),
                        'Freight_Comp': row['Compare_Freight'],
                        'is_total': False,
                        'category': category
                    })

                # Add to grand total target (for all categories)
                grand_total['Target_SQR'] += category_target_sum

                # Add category total (skip for Market Load and Other)
                if category not in ['Market Load', 'Other']:
                    final_rows.append({
                        'Client - Wise': f"{category} - Total",
                        'Target SQR': int(category_target_sum) if category_target_sum > 0 else '',
                        'Own': int(cat_df['Own_Cars'].sum()),
                        'Vendor': int(cat_df['Vendor_Cars'].sum()),
                        'Total': int(cat_df['Total_Cars'].sum()),
                        'Own_F': cat_df['Own_Freight'].sum(),
                        'Vendor_F': cat_df['Vendor_Freight'].sum(),
                        'Total_F': cat_df['Total_Freight'].sum(),
                        'Cars_Comp': int(cat_df['Compare_Cars'].sum()),
                        'Freight_Comp': cat_df['Compare_Freight'].sum(),
                        'is_total': True,
                        'category': category
                    })

                # Accumulate grand total
                grand_total['Own'] += int(cat_df['Own_Cars'].sum())
                grand_total['Vendor'] += int(cat_df['Vendor_Cars'].sum())
                grand_total['Total'] += int(cat_df['Total_Cars'].sum())
                grand_total['Own_F'] += cat_df['Own_Freight'].sum()
                grand_total['Vendor_F'] += cat_df['Vendor_Freight'].sum()
                grand_total['Total_F'] += cat_df['Total_Freight'].sum()
                grand_total['Cars_Comp'] += int(cat_df['Compare_Cars'].sum())
                grand_total['Freight_Comp'] += cat_df['Compare_Freight'].sum()

        # Add Grand Total row
        final_rows.append({
            'Client - Wise': 'Grand Total',
            'Target SQR': int(grand_total['Target_SQR']) if grand_total['Target_SQR'] > 0 else '',
            'Own': grand_total['Own'],
            'Vendor': grand_total['Vendor'],
            'Total': grand_total['Total'],
            'Own_F': grand_total['Own_F'],
            'Vendor_F': grand_total['Vendor_F'],
            'Total_F': grand_total['Total_F'],
            'Cars_Comp': grand_total['Cars_Comp'],
            'Freight_Comp': grand_total['Freight_Comp'],
            'is_total': True,
            'category': 'Grand'
        })

        # Create display dataframe with row numbers
        display_data = []
        for idx, row in enumerate(final_rows, 1):
            display_data.append({
                '': idx,
                'Client - Wise': row['Client - Wise'],
                'Target SQR': row['Target SQR'],
                'Own': row['Own'],
                'Vendor': row['Vendor'],
                'Total': row['Total'],
                'Own ': f"₹{row['Own_F']/100000:.2f}",
                'Vendor ': f"₹{row['Vendor_F']/100000:.2f}",
                'Total ': f"₹{row['Total_F']/100000:.2f}",
                'Cars': row['Cars_Comp'],
                'Freight': f"₹{row['Freight_Comp']/100000:.2f}"
            })

        display_df = pd.DataFrame(display_data)

        # Build HTML table with proper formatting
        compare_date = compare_till_date.strftime("%dth %b'%y")

        html_table = f"""
        <style>
            body {{
                background-color: #0e1117;
                color: #ffffff;
            }}
            .custom-table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
                background-color: #0e1117;
                color: #ffffff;
            }}
            .custom-table th {{
                padding: 8px 12px;
                text-align: center;
                font-weight: bold;
                color: #ffffff;
                border: 1px solid #2d3748;
                border-bottom: 2px solid #4a5568;
            }}
            .custom-table td {{
                padding: 8px 10px;
                border: 1px solid #2d3748;
                color: #ffffff;
            }}
            .custom-table tr {{
                border-bottom: 1px solid #4a5568;
            }}
            .custom-table tbody tr:hover {{
                background-color: #1a202c;
            }}
            .summary-row {{
                background-color: #1a365d !important;
            }}
            .col-divider {{
                border-left: 2px solid #3b82f6 !important;
            }}
            .col-divider-right {{
                border-right: 2px solid #3b82f6 !important;
            }}
            .header-group {{
                background-color: #1e3a5f;
                color: #ffffff;
            }}
            .header-sub {{
                background-color: #16213e;
                color: #ffffff;
                font-size: 12px;
            }}
            .total-row {{
                background-color: #d4a017 !important;
                color: #000000 !important;
                font-weight: bold;
            }}
            .total-row td {{
                color: #000000 !important;
            }}
            .grand-total-row {{
                background-color: #1e40af !important;
                color: #ffffff !important;
                font-weight: bold;
            }}
            .grand-total-row td {{
                color: #ffffff !important;
            }}
            .data-row {{
                color: #ffffff;
            }}
            .data-row:hover {{
                background-color: #2d2d44;
            }}
            .text-left {{ text-align: left; }}
            .text-right {{ text-align: right; }}
            .text-center {{ text-align: center; }}
        </style>
        <div style="overflow-x: auto;">
        <table class="custom-table">
            <thead>
                <tr>
                    <th class="header-group text-left col-divider-right" style="width: 25%;">Client - Wise</th>
                    <th class="header-group text-center col-divider-right" style="width: 8%;">Target SOB</th>
                    <th class="header-group text-center col-divider-right" colspan="3" style="background-color: #1e3a5f;">No. of Cars</th>
                    <th class="header-group text-center col-divider-right" colspan="3" style="background-color: #1e3a5f;">Freight (₹ Lakhs)</th>
                    <th class="header-group text-center" colspan="2" style="background-color: #0e4a6f;">Comparison (Till {compare_date})</th>
                </tr>
                <tr class="header-sub">
                    <th class="col-divider-right"></th>
                    <th class="col-divider-right"></th>
                    <th>Own</th>
                    <th>Vendor</th>
                    <th class="col-divider-right">Total</th>
                    <th>Own</th>
                    <th>Vendor</th>
                    <th class="col-divider-right">Total</th>
                    <th>Cars</th>
                    <th>Freight</th>
                </tr>
            </thead>
            <tbody>
        """

        for row in final_rows:
            is_total = row['is_total']
            is_grand = row['category'] == 'Grand'

            if is_grand:
                row_class = 'grand-total-row'
            elif is_total:
                row_class = 'total-row'
            else:
                row_class = 'data-row'

            html_table += f"""
                <tr class="{row_class}">
                    <td class="text-left col-divider-right">{row['Client - Wise']}</td>
                    <td class="text-center col-divider-right">{row['Target SQR']}</td>
                    <td class="text-center">{row['Own']}</td>
                    <td class="text-center">{row['Vendor']}</td>
                    <td class="text-center col-divider-right">{row['Total']}</td>
                    <td class="text-right">₹{row['Own_F']/100000:.2f}</td>
                    <td class="text-right">₹{row['Vendor_F']/100000:.2f}</td>
                    <td class="text-right col-divider-right">₹{row['Total_F']/100000:.2f}</td>
                    <td class="text-center">{row['Cars_Comp']}</td>
                    <td class="text-right">₹{row['Freight_Comp']/100000:.2f}</td>
                </tr>
            """

        # Calculate Avg Per Day and Shortfall
        days_in_period = (till_date - select_month_start).days + 1
        avg_per_day = grand_total['Total_F'] / days_in_period / 100000 if days_in_period > 0 else 0

        # Shortfall calculation (current period freight - comparison period freight)
        shortfall = (grand_total['Total_F'] - grand_total['Freight_Comp']) / 100000

        # Color based on positive/negative
        avg_color = "#22c55e" if avg_per_day >= 0 else "#ef4444"  # Green if positive, Red if negative
        shortfall_color = "#22c55e" if shortfall >= 0 else "#ef4444"  # Green if positive, Red if negative

        # Add Avg Per Day row
        html_table += f"""
                <tr class="summary-row">
                    <td colspan="7" class="text-center" style="color: #ffffff; font-weight: bold; padding: 10px; background-color: #0f172a;">Avg Per Day >></td>
                    <td colspan="3" class="text-center" style="color: {avg_color}; font-weight: bold; padding: 10px; background-color: #0f172a;">₹{avg_per_day:.2f} L</td>
                </tr>
                <tr class="summary-row">
                    <td colspan="7" class="text-center" style="color: #ffffff; font-weight: bold; padding: 10px; background-color: #0f172a;">Shortfall from Till {compare_date} >></td>
                    <td colspan="3" class="text-center" style="color: {shortfall_color}; font-weight: bold; padding: 10px; background-color: #0f172a;">₹{shortfall:.2f} L</td>
                </tr>
            </tbody>
        </table>
        </div>
        """

        # Calculate dynamic height based on number of rows + summary rows
        table_height = 100 + (len(final_rows) * 35) + 100
        components.html(html_table, height=table_height, scrolling=False)

    with tab2:
        st.markdown("### Daily Loading Details")

        @st.fragment
        def daily_loading_fragment():
            # Date filter and summary in one row
            col_date, col_cars, col_trips_count, col_empty = st.columns([1, 1, 1, 1])
            with col_date:
                selected_date = st.date_input("Select Date", datetime.now().date(), key='daily_date_fragment')

            # Filter data for selected date - only loaded trips (with Party Name)
            daily_data = df[
                (df['LoadingDateOnly'] == selected_date) &
                (df['DisplayParty'] != '') &
                (df['DisplayParty'].notna()) &
                (df['CarQty'] > 0)
            ]

            # Total Cars Lifted
            total_cars_lifted = int(daily_data['CarQty'].sum())
            total_trips_count = len(daily_data)

            with col_cars:
                st.markdown(f"""
                <div style="background: linear-gradient(90deg, #22c55e, #16a34a); padding: 15px 25px; border-radius: 8px; text-align: center; margin-top: 25px;">
                    <span style="color: white; font-weight: bold; font-size: 20px;">Total Cars Lifted: {total_cars_lifted}</span>
                </div>
                """, unsafe_allow_html=True)

            with col_trips_count:
                st.markdown(f"""
                <div style="background: linear-gradient(90deg, #3b82f6, #2563eb); padding: 15px 25px; border-radius: 8px; text-align: center; margin-top: 25px;">
                    <span style="color: white; font-weight: bold; font-size: 20px;">Total Trips: {total_trips_count}</span>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # Create two columns - Trip Details and Party-wise Summary
            col_left, col_right = st.columns([3, 1])

            with col_left:
                st.markdown(f"**Daily Loading Details {selected_date.strftime('%d-%b-%Y')}**")

                if len(daily_data) > 0:
                    # Prepare trip details table
                    trip_details = daily_data[['LoadingDate', 'VehicleNo', 'DisplayParty', 'Route', 'CarQty']].copy()

                    # Split Route into Origin and Destination
                    trip_details['Origin'] = trip_details['Route'].apply(lambda x: x.split(' - ')[0] if ' - ' in str(x) else str(x))
                    trip_details['Destination'] = trip_details['Route'].apply(lambda x: x.split(' - ')[1] if ' - ' in str(x) and len(x.split(' - ')) > 1 else '')

                    # Build HTML table
                    html_trips = """
                    <style>
                        .trips-table { width: 100%; border-collapse: collapse; font-size: 13px; }
                        .trips-table th { background-color: #1e3a5f; color: white; padding: 10px; text-align: left; border: 1px solid #3b82f6; }
                        .trips-table td { padding: 8px 10px; border: 1px solid #2d3748; color: white; }
                        .trips-table tr:nth-child(even) { background-color: #1a1f2e; }
                        .trips-table tr:nth-child(odd) { background-color: #0e1117; }
                        .trips-table tr:hover { background-color: #2d3748; }
                    </style>
                    <div style="max-height: 450px; overflow-y: auto;">
                    <table class="trips-table">
                        <thead>
                            <tr>
                                <th>S.No.</th>
                                <th>Date</th>
                                <th>Vehicle No</th>
                                <th>Party Name</th>
                                <th>Origin</th>
                                <th>Destination</th>
                                <th>Qty</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                    total_qty = 0
                    for idx, (_, row) in enumerate(trip_details.iterrows(), 1):
                        date_str = row['LoadingDate'].strftime('%d/%m/%Y')
                        qty = int(row['CarQty'])
                        total_qty += qty
                        html_trips += f"""
                            <tr>
                                <td>{idx}</td>
                                <td>{date_str}</td>
                                <td>{row['VehicleNo']}</td>
                                <td>{row['DisplayParty']}</td>
                                <td>{row['Origin']}</td>
                                <td>{row['Destination']}</td>
                                <td style="text-align: center;">{qty}</td>
                            </tr>
                        """
                    # Add Grand Total row
                    html_trips += f"""
                            <tr style="background-color: #1e40af !important; font-weight: bold;">
                                <td colspan="6" style="text-align: right; color: white;">Grand Total</td>
                                <td style="text-align: center; color: #fbbf24; font-size: 15px;">{total_qty}</td>
                            </tr>
                        """
                    html_trips += "</tbody></table></div>"
                    components.html(html_trips, height=500, scrolling=True)
                else:
                    st.info("No trips found for the selected date.")

            with col_right:
                st.markdown(f"**OEM's Wise Total Loads {selected_date.strftime('%d-%b-%Y')}**")

                if len(daily_data) > 0:
                    # Party-wise summary
                    party_summary = daily_data.groupby('DisplayParty').agg({
                        'TLHSNo': 'count'
                    }).reset_index()
                    party_summary.columns = ['Party Name', 'Trip Count']
                    party_summary = party_summary[party_summary['Party Name'] != '']
                    party_summary = party_summary.sort_values('Trip Count', ascending=False)

                    # Build HTML table
                    html_summary = """
                    <style>
                        .summary-table { width: 100%; border-collapse: collapse; font-size: 13px; }
                        .summary-table th { background-color: #1e3a5f; color: white; padding: 10px; text-align: left; border: 1px solid #3b82f6; }
                        .summary-table td { padding: 8px 10px; border: 1px solid #2d3748; color: white; }
                        .summary-table tr:nth-child(even) { background-color: #1a1f2e; }
                        .summary-table tr:nth-child(odd) { background-color: #0e1117; }
                        .summary-table tr:hover { background-color: #2d3748; }
                    </style>
                    <div style="max-height: 450px; overflow-y: auto;">
                    <table class="summary-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Party Name</th>
                                <th>Trip Count</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                    total_trip_count = 0
                    for idx, (_, row) in enumerate(party_summary.iterrows(), 1):
                        trip_count = row['Trip Count']
                        total_trip_count += trip_count
                        html_summary += f"""
                            <tr>
                                <td>{idx}</td>
                                <td>{row['Party Name']}</td>
                                <td style="text-align: center; font-weight: bold; color: #22c55e;">{trip_count}</td>
                            </tr>
                        """
                    # Add Grand Total row
                    html_summary += f"""
                            <tr style="background-color: #1e40af !important; font-weight: bold;">
                                <td colspan="2" style="text-align: right; color: white;">Grand Total</td>
                                <td style="text-align: center; color: #fbbf24; font-size: 15px;">{total_trip_count}</td>
                            </tr>
                        """
                    html_summary += "</tbody></table></div>"
                    components.html(html_summary, height=500, scrolling=True)
                else:
                    st.info("No data available.")

        # Call the fragment
        daily_loading_fragment()

    with tab3:
        st.markdown("### Local/Pilot Loads")

        # Use fragment to prevent tab switching on filter change
        @st.fragment
        def local_pilot_fragment():
            # Define KIA Local vehicle numbers
            kia_vehicles = ['4068 NL01N', '4388 NL01AJ', '4390 NL01AJ', '9454 NL01L', '9456 NL01L',
                            '5307 NL01N', '0218 NL01AH', '9453 NL01L', '0167 NL01AH']

            # Create filter functions for each category
            def get_toyota_local(data):
                return data[data['NewPartyName'].str.contains('DC Movement', case=False, na=False)]

            def get_patna_local(data):
                return data[data['NewPartyName'].str.contains('MAHINDRA LOGISTICS LTD.*Train Load', case=False, na=False, regex=True)]

            def get_haridwar_local(data):
                return data[data['VehicleNo'].str.contains('8630 NL01AG', case=False, na=False)]

            def get_road_pilot(data):
                return data[data['DriverName'].str.contains('road pilot', case=False, na=False)]

            def get_kia_local(data):
                pattern = '|'.join([v.replace(' ', '.*') for v in kia_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            # Filter month_df to only include loaded trips
            loaded_month_df = month_df[
                (month_df['DisplayParty'] != '') &
                (month_df['DisplayParty'].notna()) &
                (month_df['CarQty'] > 0)
            ]

            # Get data for each category (using loaded trips only)
            toyota_local = get_toyota_local(loaded_month_df)
            patna_local = get_patna_local(loaded_month_df)
            haridwar_local = get_haridwar_local(loaded_month_df)
            road_pilot = get_road_pilot(loaded_month_df)
            kia_local = get_kia_local(loaded_month_df)

            # Summary data for all categories
            summary_data = [
                {'Category': 'Toyota Local', 'Trips': len(toyota_local), 'Cars': int(toyota_local['CarQty'].sum()), 'Freight': toyota_local['Freight'].sum()},
                {'Category': 'Patna Local', 'Trips': len(patna_local), 'Cars': int(patna_local['CarQty'].sum()), 'Freight': patna_local['Freight'].sum()},
                {'Category': 'Haridwar Local', 'Trips': len(haridwar_local), 'Cars': int(haridwar_local['CarQty'].sum()), 'Freight': haridwar_local['Freight'].sum()},
                {'Category': 'Road Pilot', 'Trips': len(road_pilot), 'Cars': int(road_pilot['CarQty'].sum()), 'Freight': road_pilot['Freight'].sum()},
                {'Category': 'Kia Local', 'Trips': len(kia_local), 'Cars': int(kia_local['CarQty'].sum()), 'Freight': kia_local['Freight'].sum()},
            ]

            # Summary Section
            st.markdown("#### Summary")
            summary_html = """
            <style>
                .summary-local { width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 20px; border: 2px solid #3b82f6; }
                .summary-local th { background-color: #1e3a5f; color: white; padding: 12px; text-align: center; border: 1px solid #3b82f6; }
                .summary-local td { padding: 10px; border: 1px solid #2d3748; color: white; text-align: center; }
                .summary-local tr:nth-child(even) { background-color: #1a1f2e; }
                .summary-local tr:nth-child(odd) { background-color: #0e1117; }
                .summary-local .total-row { background-color: #1e40af !important; font-weight: bold; }
                .summary-local .total-row td { border: 1px solid #3b82f6; border-bottom: 2px solid #3b82f6; }
            </style>
            <table class="summary-local">
                <thead>
                    <tr>
                        <th>Category</th>
                        <th>Total Trips</th>
                        <th>Cars Lifted</th>
                        <th>Freight</th>
                    </tr>
                </thead>
                <tbody>
            """
            total_trips = 0
            total_cars = 0
            total_freight = 0
            for item in summary_data:
                total_trips += item['Trips']
                total_cars += item['Cars']
                total_freight += item['Freight']
                summary_html += f"""
                    <tr>
                        <td style="text-align: left; font-weight: bold;">{item['Category']}</td>
                        <td>{item['Trips']}</td>
                        <td>{item['Cars']}</td>
                        <td>₹{item['Freight']/100000:.2f}L</td>
                    </tr>
                """
            summary_html += f"""
                    <tr class="total-row">
                        <td style="text-align: left;">Grand Total</td>
                        <td style="color: #fbbf24;">{total_trips}</td>
                        <td style="color: #fbbf24;">{total_cars}</td>
                        <td style="color: #fbbf24;">₹{total_freight/100000:.2f}L</td>
                    </tr>
                </tbody>
            </table>
            """
            components.html(summary_html, height=320)

            # Filter dropdown
            st.markdown("#### Details by Category")
            col_filter, col_empty = st.columns([1, 3])
            with col_filter:
                category_options = ['Toyota Local', 'Patna Local', 'Haridwar Local', 'Road Pilot', 'Kia Local']
                selected_category = st.selectbox("Select Category", category_options, key='local_category')

            # Get filtered data based on selection
            if selected_category == 'Toyota Local':
                filtered_df = toyota_local
            elif selected_category == 'Patna Local':
                filtered_df = patna_local
            elif selected_category == 'Haridwar Local':
                filtered_df = haridwar_local
            elif selected_category == 'Road Pilot':
                filtered_df = road_pilot
            else:
                filtered_df = kia_local

            if len(filtered_df) > 0:
                # Build details table
                details_html = """
                <style>
                    .details-table { width: 100%; border-collapse: collapse; font-size: 13px; }
                    .details-table th { background-color: #1e3a5f; color: white; padding: 10px; text-align: left; border: 1px solid #3b82f6; }
                    .details-table td { padding: 8px 10px; border: 1px solid #2d3748; color: white; }
                    .details-table tr:nth-child(even) { background-color: #1a1f2e; }
                    .details-table tr:nth-child(odd) { background-color: #0e1117; }
                    .details-table tr:hover { background-color: #2d3748; }
                    .details-table .total-row { background-color: #1e40af !important; font-weight: bold; }
                </style>
                <div style="max-height: 400px; overflow-y: auto;">
                <table class="details-table">
                    <thead>
                        <tr>
                            <th>S.No.</th>
                            <th>Vehicle No</th>
                            <th>Date</th>
                            <th>Route</th>
                            <th>Freight</th>
                            <th>Qty</th>
                        </tr>
                    </thead>
                    <tbody>
                """
                total_freight_detail = 0
                total_qty_detail = 0
                for idx, (_, row) in enumerate(filtered_df.iterrows(), 1):
                    date_str = row['LoadingDate'].strftime('%d/%m/%Y') if pd.notna(row['LoadingDate']) else ''
                    freight = row['Freight'] if pd.notna(row['Freight']) else 0
                    qty = int(row['CarQty']) if pd.notna(row['CarQty']) else 0
                    total_freight_detail += freight
                    total_qty_detail += qty
                    details_html += f"""
                        <tr>
                            <td>{idx}</td>
                            <td>{row['VehicleNo']}</td>
                            <td>{date_str}</td>
                            <td>{row['Route']}</td>
                            <td style="text-align: right;">₹{freight:,.0f}</td>
                            <td style="text-align: center;">{qty}</td>
                        </tr>
                    """
                details_html += f"""
                        <tr class="total-row">
                            <td colspan="4" style="text-align: right; color: white;">Grand Total</td>
                            <td style="text-align: right; color: #fbbf24;">₹{total_freight_detail:,.0f}</td>
                            <td style="text-align: center; color: #fbbf24;">{total_qty_detail}</td>
                        </tr>
                    </tbody>
                </table>
                </div>
                """
                components.html(details_html, height=450, scrolling=True)
            else:
                st.info(f"No data found for {selected_category}.")

        # Call the fragment
        local_pilot_fragment()

    with tab4:
        st.markdown("### Zone View")

        # Zone mapping function
        def get_zone(city):
            if pd.isna(city) or city == '':
                return 'Unknown'
            city_upper = str(city).upper().strip()

            # North Zone - Delhi NCR, Punjab, Haryana, Himachal, J&K, Uttarakhand, Rajasthan
            north_cities = ['DELHI', 'NOIDA', 'GURGAON', 'GURUGRAM', 'FARIDABAD', 'GHAZIABAD', 'GREATER NOIDA',
                           'CHANDIGARH', 'MOHALI', 'PANCHKULA', 'LUDHIANA', 'JALANDHAR', 'JALLANDHAR', 'AMRITSAR', 'PATIALA',
                           'BATHINDA', 'BHATINDA', 'FIROZPUR', 'FEROZEPUR', 'HOSHIARPUR', 'MUKTSAR', 'SANGRUR',
                           'SONIPAT', 'PANIPAT', 'KARNAL', 'KURUKSHETRA', 'AMBALA', 'HISAR', 'HISSAR', 'ROHTAK',
                           'BHIWANI', 'SIRSA', 'JIND', 'KAITHAL', 'REWARI', 'NARNAUL', 'BAHADURGARH', 'DHARUHERA',
                           'MANESAR', 'KUNDLI', 'PALWAL', 'NUH', 'FARRUKHNAGAR', 'FARUKHNAGAR', 'PIRTHLA',
                           'SHIMLA', 'MANDI', 'KANGRA', 'KULLU', 'SOLAN', 'UNA', 'HAMIRPUR', 'PAONTA SAHIB',
                           'JAMMU', 'SRINAGAR', 'KATHUA', 'PATHANKOT',
                           'DEHRADUN', 'HARIDWAR', 'ROORKEE', 'HALDWANI', 'RUDRAPUR', 'KASHIPUR',
                           'TAPUKERA', 'ICAT MANESAR', 'KHARKHODA', 'BILASPUR (HR)', 'YAMUNANAGAR',
                           'JAIPUR', 'JODHPUR', 'UDAIPUR', 'KOTA', 'AJMER', 'BIKANER', 'ALWAR', 'BHILWARA',
                           'SIKAR', 'SRIGANGANAGAR', 'BHARATPUR', 'DAUSA', 'JHUNJHUNU', 'CHITTORGARH', 'TONK',
                           'BANSWARA', 'NAGAUR', 'SAWAI MADHOPUR', 'JHALAWAR', 'NEEMUCH']

            # East Zone - West Bengal, Bihar, Jharkhand, Odisha, Assam, NE States
            east_cities = ['KOLKATA', 'HOWRAH', 'SILIGURI', 'ASANSOL', 'DURGAPUR', 'KHARAGPUR', 'MALDA',
                          'BARDHAMAN', 'BARDDHAMAN', 'BEHRAMPORE', 'BERHAMPORE', 'COOCHBEHAR', 'ALIPURDUAR',
                          'PATNA', 'MUZAFFARPUR', 'GAYA', 'BHAGALPUR', 'DARBHANGA', 'BEGUSARAI', 'CHAPRA',
                          'MOTIHARI', 'PURNIA', 'SAMASTIPUR', 'BIHAR SHARIF', 'SAHARSA', 'SIWAN', 'GOPALGANJ',
                          'GOPALGUNJ', 'ARRAH', 'AURANGABAD (BIHAR)', 'VAISHALI', 'KISHANGANJ', 'JAMALPUR',
                          'RANCHI', 'JAMSHEDPUR', 'DHANBAD', 'BOKARO', 'HAZARIBAGH', 'HAZARIBAG', 'DEOGHAR',
                          'DALTONGANJ', 'BARHI(JH)',
                          'BHUBANESHWAR', 'CUTTACK', 'ROURKELA', 'SAMBALPUR', 'BERHAMPUR', 'BRAHMAPUR',
                          'BALASORE', 'PURI', 'ANGUL', 'PANIKOILI', 'KEONJHAR', 'JEYPORE', 'JAYPORE',
                          'GUWAHATI', 'TEZPUR', 'DIBRUGARH', 'JORHAT', 'NAGAON', 'BONGAIGAON', 'NORTH LAKHIMPUR',
                          'SHILLONG', 'GANGTOK', 'DIMAPUR', 'NAHARLAGUN', 'WEST CHAMPARAN']

            # West Zone - Maharashtra, Gujarat, Goa
            west_cities = ['MUMBAI', 'PUNE', 'NASHIK', 'NAGPUR', 'AURANGABAD', 'AURNGABAD(MH)', 'AURANGABAD(MAHARASHTRA)',
                          'SOLAPUR', 'KOLHAPUR', 'SANGLI', 'SATARA', 'AHMEDNAGAR', 'THANE', 'NAVI MUMBAI',
                          'BHIWANDI', 'PANVEL', 'PANWEL', 'VASAI', 'KALYAN', 'RATNAGIRI', 'LATUR', 'BEED',
                          'JALGAON', 'DHULE', 'AKOLA', 'AMRAVATI', 'CHANDRAPUR', 'CHANDERPUR', 'MALEGAON',
                          'BARAMATI', 'SANGAMNER', 'RANJANGAON', 'UDGIR', 'NANDED',
                          'AHMEDABAD', 'SURAT', 'VADODARA', 'RAJKOT', 'BHAVNAGAR', 'JAMNAGAR', 'JUNAGADH',
                          'GANDHIDHAM', 'BHUJ', 'ANAND', 'MEHSANA', 'MORBI', 'VAPI', 'NAVSARI', 'BHARUCH',
                          'ANKLESHWAR', 'GODHRA', 'DAHOD', 'HIMATNAGAR', 'HIMMATNAGAR', 'SURENDRANAGAR',
                          'PALANPUR', 'SANAND', 'BECHRAJI', 'HALOL', 'BARDOLI', 'CHHARODI', 'AMBLI', 'GANDHINAGAR',
                          'DHOLERA', 'PIPAVAV PORT', 'KHEDA(GJ)',
                          'GOA', 'NUVEM']

            # South Zone - Karnataka, Tamil Nadu, Kerala, Andhra Pradesh, Telangana
            south_cities = ['BANGALORE', 'MYSORE', 'HUBLI', 'BELGAUM', 'BELGAON', 'MANGALORE', 'MANGLORE',
                           'DAVANGERE', 'BELLARY', 'TUMKUR', 'SHIMOGA', 'SIMOGA', 'HASSAN', 'UDUPI',
                           'CHITRADURGA', 'HOSPET', 'GULBARGA', 'KALABURGI', 'BIJAPUR', 'RAICHUR',
                           'CHIKMAGALUR', 'CHIKKAMAGALURU', 'CHIKKABALLAPUR', 'RAMANAGARA', 'BIDADI',
                           'HOSUR', 'KADUR', 'SINDHANUR', 'YELLAPUR(KA)', 'TOYOTA BANGLORE', 'HAROHALLI',
                           'CHENNAI', 'COIMBATORE', 'MADURAI', 'TRICHY', 'SALEM', 'TIRUPUR', 'ERODE',
                           'VELLORE', 'TIRUNELVELI', 'NAGERCOIL', 'THANJAVUR', 'CUDDALORE', 'KANCHIPURAM',
                           'PONDICHERRY', 'PUDUCHERRY', 'KARAIKUDI', 'SRI CITY', 'CHENNAI PORT', 'CHENNAI TI',
                           'VILUPPURAM', 'VERA VILPUR', 'HYUNDAI',
                           'KOCHI', 'COCHIN', 'KOCHIN', 'THIRUVANANTHAPURAM', 'TRIVANDRUM', 'KOZHIKODE',
                           'CALICUT', 'THRISSUR', 'KOLLAM', 'ALAPPUZHA', 'ALLEPPHY', 'PALAKKAD', 'PALLAKAD',
                           'KANNUR', 'KASARGOD', 'KOTTAYAM', 'PATHANAMTHITTA', 'KAYAMKULAM', 'MALAPPURAM',
                           'ERNAKULAM', 'MUVATTUPUZHA',
                           'HYDERABAD', 'SECUNDERABAD', 'VIJAYAWADA', 'VISAKHAPATNAM', 'VISHAKHAPATNAM',
                           'TIRUPATI', 'GUNTUR', 'NELLORE', 'KURNOOL', 'KADAPA', 'ANANTAPUR', 'ONGOLE',
                           'RAJAHMUNDRY', 'KAKINADA', 'BHIMAVARAM', 'SRIKAKULAM', 'ANAKAPALLI', 'KIA',
                           'WARANGAL', 'KARIMNAGAR', 'NIZAMABAD', 'KHAMMAM', 'NALGONDA', 'MAHBUBNAGAR',
                           'NIRMAL', 'ZAHEERABAD', 'ADONI']

            # Central Zone - Madhya Pradesh, Chhattisgarh, UP (Central/East)
            central_cities = ['BHOPAL', 'INDORE', 'JABALPUR', 'GWALIOR', 'UJJAIN', 'RATLAM', 'DEWAS', 'SAGAR',
                             'SATNA', 'REWA', 'KATNI', 'CHHINDWARA', 'CHINDWARA', 'KHANDWA', 'KHARGONE',
                             'HOSHANGABAD', 'SEHORE', 'VIDISHA', 'SHAHDOL', 'SEONI', 'LAKHNADON', 'SHIVPURI',
                             'GUNA', 'BIAORA', 'SHUJALPUR', 'SUJALPUR', 'CHHATARPUR', 'MAHOBA', 'WAIDHAN',
                             'JHABUA', 'JABHUA', 'NEEMUCH',
                             'RAIPUR', 'BILASPUR', 'BHILAI', 'KORBA', 'RAJNANDGAON', 'DURG', 'JAGDALPUR',
                             'AMBIKAPUR', 'KANKER',
                             'LUCKNOW', 'KANPUR', 'AGRA', 'VARANASI', 'ALLAHABAD', 'PRAYAGRAJ', 'GORAKHPUR',
                             'BAREILLY', 'ALIGARH', 'MORADABAD', 'MEERUT', 'SAHARANPUR', 'MATHURA', 'FIROZABAD',
                             'ETAWAH', 'ETAH', 'MAINPURI', 'SHAHJAHANPUR', 'SAHANJANPUR', 'SITAPUR', 'HARDOI',
                             'UNNAO', 'RAE BAREILLY', 'RAEBARELI', 'RAI BARELI', 'SULTANPUR', 'FAIZABAD',
                             'AZAMGARH', 'JAUNPUR', 'MIRZAPUR', 'ROBERTSGANJ', 'BASTI', 'GONDA', 'DEORIA',
                             'BULANDSHAHAR', 'BIJNOR', 'MUZAFFARNAGAR', 'MUZAFFAR NAGAR', 'NAJIBABAD',
                             'LAKHIMPUR', 'LAKHIMPUR KHERI', 'ORAI', 'JHANSI', 'FARRUKHABAD', 'PRATAPGARH',
                             'ABOHAR', 'KUNDA', 'KHURJA']

            # Check each zone
            for city_check in north_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'North'

            for city_check in east_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'East'

            for city_check in west_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'West'

            for city_check in south_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'South'

            for city_check in central_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'Central'

            return 'Other'

        # Filter loaded trips only
        loaded_df = month_df[
            (month_df['DisplayParty'] != '') &
            (month_df['DisplayParty'].notna()) &
            (month_df['CarQty'] > 0)
        ].copy()

        # Extract Origin and Destination from Route
        loaded_df['Origin'] = loaded_df['Route'].apply(lambda x: str(x).split(' - ')[0].strip() if ' - ' in str(x) else str(x).strip())
        loaded_df['Destination'] = loaded_df['Route'].apply(lambda x: str(x).split(' - ')[1].strip() if ' - ' in str(x) and len(str(x).split(' - ')) > 1 else '')

        # Map to zones
        loaded_df['Origin_Zone'] = loaded_df['Origin'].apply(get_zone)
        loaded_df['Dest_Zone'] = loaded_df['Destination'].apply(get_zone)

        # Create pivot tables
        zones = ['Central', 'East', 'North', 'South', 'West']

        # Build the zone matrix for Cars Lifted
        cars_matrix = {}
        for origin_zone in zones:
            cars_matrix[origin_zone] = {}
            for dest_zone in zones:
                count = loaded_df[(loaded_df['Origin_Zone'] == origin_zone) & (loaded_df['Dest_Zone'] == dest_zone)]['CarQty'].sum()
                cars_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

        # Build the zone matrix for Trips Count
        trips_matrix = {}
        for origin_zone in zones:
            trips_matrix[origin_zone] = {}
            for dest_zone in zones:
                count = len(loaded_df[(loaded_df['Origin_Zone'] == origin_zone) & (loaded_df['Dest_Zone'] == dest_zone)])
                trips_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

        # Function to get text color based on value (green for high, red for low)
        def get_text_color(val, max_val):
            if val == 0 or max_val == 0:
                return 'color: white;'
            ratio = val / max_val
            if ratio >= 0.7:
                return 'color: #22c55e; font-weight: bold;'  # Bright green
            elif ratio >= 0.4:
                return 'color: #84cc16; font-weight: bold;'  # Light green
            elif ratio >= 0.2:
                return 'color: #eab308; font-weight: bold;'  # Yellow
            else:
                return 'color: #ef4444; font-weight: bold;'  # Red

        # Get max values for color scaling
        cars_values = [v for row in cars_matrix.values() for v in row.values() if v > 0]
        trips_values = [v for row in trips_matrix.values() for v in row.values() if v > 0]
        max_cars = max(cars_values) if cars_values else 1
        max_trips = max(trips_values) if trips_values else 1

        # Function to build zone table HTML
        def build_zone_table(matrix, title, max_val):
            html = f"""
            <h4 style="color: white; margin-bottom: 10px;">{title}</h4>
            <style>
                .zone-table {{ width: 100%; border-collapse: collapse; font-size: 14px; border: 2px solid #3b82f6; }}
                .zone-table th {{ background-color: #1e3a5f; color: white; padding: 12px; text-align: center; border: 1px solid #3b82f6; }}
                .zone-table td {{ padding: 10px; border: 1px solid #2d3748; color: white; text-align: center; }}
                .zone-table tr:nth-child(even) {{ background-color: #1a1f2e; }}
                .zone-table tr:nth-child(odd) {{ background-color: #0e1117; }}
                .zone-table .total-row {{ background-color: #1e40af !important; font-weight: bold; }}
                .zone-table .total-row td {{ border: 1px solid #3b82f6; }}
                .zone-table .row-header {{ text-align: left; font-weight: bold; font-style: italic; }}
                .zone-table .grand-total {{ color: #fbbf24; font-weight: bold; }}
            </style>
            <table class="zone-table">
                <thead>
                    <tr>
                        <th style="font-style: italic;">Origin Zone</th>
            """
            for dest_zone in zones:
                html += f'<th>{dest_zone}</th>'
            html += '<th>Grand Total</th></tr></thead><tbody>'

            grand_total = 0
            col_totals = {z: 0 for z in zones}

            for origin_zone in zones:
                row_total = 0
                html += f'<tr><td class="row-header">{origin_zone}</td>'
                for dest_zone in zones:
                    val = matrix[origin_zone][dest_zone]
                    if val > 0:
                        row_total += val
                        col_totals[dest_zone] += val
                        text_style = get_text_color(val, max_val)
                        html += f'<td style="{text_style}">{val}</td>'
                    else:
                        html += '<td></td>'
                grand_total += row_total
                html += f'<td style="font-weight: bold; color: white;">{row_total}</td></tr>'

            html += '<tr class="total-row"><td class="row-header">Grand Total</td>'
            for dest_zone in zones:
                html += f'<td class="grand-total">{col_totals[dest_zone]}</td>'
            html += f'<td class="grand-total">{grand_total}</td></tr>'

            html += '</tbody></table>'
            return html

        # Display both tables side by side
        col_table1, col_table2 = st.columns(2)

        with col_table1:
            st.markdown("#### No. of Cars Lifted")
            cars_html = build_zone_table(cars_matrix, "", max_cars)
            components.html(cars_html, height=280)

        with col_table2:
            st.markdown("#### No. of Loaded Trips")
            trips_html = build_zone_table(trips_matrix, "", max_trips)
            components.html(trips_html, height=280)

        # Chart: Zone by Car Lifted (excluding DC Movement)
        st.markdown("---")
        st.markdown("#### Zone by Car Lifted")
        st.caption("*Note: DC Movement not included*")

        import plotly.graph_objects as go

        # Filter out DC Movement for this chart
        chart_df = loaded_df[~loaded_df['NewPartyName'].str.contains('DC Movement', case=False, na=False)]

        # Recalculate zone matrix excluding DC Movement
        chart_cars_matrix = {}
        for origin_zone in zones:
            chart_cars_matrix[origin_zone] = {}
            for dest_zone in zones:
                count = chart_df[(chart_df['Origin_Zone'] == origin_zone) & (chart_df['Dest_Zone'] == dest_zone)]['CarQty'].sum()
                chart_cars_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

        # Get top routes for chart
        route_data = []
        for oz in zones:
            for dz in zones:
                if chart_cars_matrix[oz][dz] > 0:
                    route_data.append({
                        'Route': f'{oz} → {dz}',
                        'Cars': chart_cars_matrix[oz][dz]
                    })

        if route_data:
            route_df = pd.DataFrame(route_data).sort_values('Cars', ascending=True).tail(10)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=route_df['Route'],
                x=route_df['Cars'],
                orientation='h',
                marker_color=['#22c55e' if c >= route_df['Cars'].quantile(0.7) else '#3b82f6' if c >= route_df['Cars'].quantile(0.4) else '#eab308' for c in route_df['Cars']],
                text=route_df['Cars'],
                textposition='outside'
            ))
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                height=400,
                xaxis_title='Cars Lifted',
                yaxis_title='',
                margin=dict(l=100, r=50, t=20, b=50)
            )
            fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig, use_container_width=True)

        # Show unmapped cities if any
        other_origins = loaded_df[loaded_df['Origin_Zone'] == 'Other']['Origin'].unique()
        other_dests = loaded_df[loaded_df['Dest_Zone'] == 'Other']['Destination'].unique()

        if len(other_origins) > 0 or len(other_dests) > 0:
            with st.expander("Unmapped Cities (Other Zone)"):
                if len(other_origins) > 0:
                    st.write("**Origins:**", ', '.join(sorted(set(other_origins))))
                if len(other_dests) > 0:
                    st.write("**Destinations:**", ', '.join(sorted(set(other_dests))))

    # Download section
    st.markdown("---")
    csv = month_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Data as CSV",
        data=csv,
        file_name=f"swift_trip_log_{selected_month.strftime('%Y%m')}.csv",
        mime="text/csv"
    )


if __name__ == "__main__":
    main()
