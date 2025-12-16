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


@st.cache_data(ttl=300)
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

    # Target SQR Update Section
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Update Target SQR")

    with st.sidebar.expander("Set Target SQR", expanded=False):
        # Get unique parties for target setting
        unique_parties = sorted(df['DisplayParty'].dropna().unique().tolist())
        unique_parties = [p for p in unique_parties if p != '']

        # Select party to update
        target_party = st.selectbox("Select Party", [''] + unique_parties, key='target_party')

        if target_party:
            current_target = targets.get(target_party, 0)
            new_target = st.number_input("Target SQR", min_value=0, value=int(current_target), key='new_target')

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

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Target vs Actual", "📅 Daily Loading Details", "🚚 Local/Pilot Loads"])

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

        # Daily summary
        daily_df = month_df.groupby(month_df['LoadingDate'].dt.date).agg({
            'TLHSNo': 'count',
            'CarQty': 'sum',
            'Freight': 'sum',
            'Distance': 'sum'
        }).reset_index()
        daily_df.columns = ['Date', 'Trips', 'Cars', 'Freight', 'Distance']
        daily_df['Freight'] = daily_df['Freight'].apply(lambda x: f"₹{x/100000:.2f}L")

        st.dataframe(daily_df, use_container_width=True, height=400)

        # Chart
        import plotly.express as px

        chart_df = month_df.groupby(month_df['LoadingDate'].dt.date).agg({
            'CarQty': 'sum'
        }).reset_index()
        chart_df.columns = ['Date', 'Cars']

        if not chart_df.empty:
            fig = px.bar(chart_df, x='Date', y='Cars', title='Daily Cars Loaded')
            fig.update_layout(template='plotly_dark', height=300)
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.markdown("### Local/Pilot Loads")

        # Filter for local/pilot trips
        local_df = month_df[
            (month_df['Route'].str.contains('LOCAL|PILOT|YARD', case=False, na=False)) |
            (month_df['Distance'] < 100)
        ]

        if len(local_df) > 0:
            local_summary = local_df.groupby('DisplayParty').agg({
                'TLHSNo': 'count',
                'CarQty': 'sum',
                'Freight': 'sum'
            }).reset_index()
            local_summary.columns = ['Party', 'Trips', 'Cars', 'Freight']
            local_summary['Freight'] = local_summary['Freight'].apply(lambda x: f"₹{x/100000:.2f}L")
            local_summary = local_summary[local_summary['Party'] != '']

            st.dataframe(local_summary, use_container_width=True, height=400)
        else:
            st.info("No local/pilot loads found for the selected period.")

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
