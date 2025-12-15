import streamlit as st
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


@st.cache_data(ttl=300)
def load_targets():
    """Load target SQR data from JSON file"""
    try:
        with open('/Users/swiftroadlink/Documents/DE/sob_targets.json', 'r') as f:
            targets = json.load(f)
        return targets
    except Exception as e:
        st.error(f"Error loading targets: {e}")
        return {}


def get_client_category(party_name):
    """Categorize clients into groups based on NewPartyName"""
    if pd.isna(party_name) or party_name == "":
        return "Other"
    party_upper = str(party_name).upper()
    if "HONDA" in party_upper:
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

    # Filter data for selected month
    month_start = selected_month.replace(day=1)
    month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)

    month_df = df[(df['LoadingDate'] >= month_start) & (df['LoadingDate'] <= month_end)]

    if selected_party != 'All':
        month_df = month_df[month_df['DisplayParty'] == selected_party]

    # Month Summary Section
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
            till_date = st.date_input("Till Date", min(datetime.now().date(), month_end.date()))
        with col3:
            compare_month = st.date_input("Compare With Month", (month_start - relativedelta(months=2)).date())
        with col4:
            compare_till_date = st.date_input("Compare Till Date", (pd.to_datetime(compare_month) + timedelta(days=till_date.day - 1)).date())

        # Filter data for current period
        current_df = df[
            (df['LoadingDate'] >= pd.to_datetime(select_month_start)) &
            (df['LoadingDate'] <= pd.to_datetime(till_date)) &
            (df['DisplayParty'] != '') &
            (df['DisplayParty'].notna())
        ]

        # Filter data for comparison period
        compare_df = df[
            (df['LoadingDate'] >= pd.to_datetime(compare_month)) &
            (df['LoadingDate'] <= pd.to_datetime(compare_till_date)) &
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
            'Other': '#6b7280'
        }

        final_rows = []
        for category in category_order:
            cat_df = summary[summary['category'] == category].copy()
            if len(cat_df) > 0:
                # Add individual rows
                for _, row in cat_df.iterrows():
                    target_sqr = targets.get(row['Party'], '')
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

                # Add category total
                total_target = targets.get(f"{category} - Total", sum(targets.get(p, 0) for p in cat_df['Party']))
                final_rows.append({
                    'Client - Wise': f"{category} - Total",
                    'Target SQR': int(total_target) if total_target else int(cat_df['Trips'].sum()),
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

        # Create display dataframe
        display_data = []
        for row in final_rows:
            display_data.append({
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

        # Style function
        def highlight_totals(row):
            if 'Total' in str(row['Client - Wise']):
                for fr in final_rows:
                    if fr['Client - Wise'] == row['Client - Wise']:
                        color = category_colors.get(fr['category'], '#6b7280')
                        return [f'background-color: {color}; color: white; font-weight: bold'] * len(row)
            return [''] * len(row)

        # Display styled dataframe
        styled_df = display_df.style.apply(highlight_totals, axis=1)
        st.dataframe(styled_df, use_container_width=True, height=500)

        # Column headers
        st.markdown("""
        **Columns:** No. of Cars (Own | Vendor | Total) | Freight ₹ Lakhs (Own | Vendor | Total) | Comparison (Cars | Freight)
        """)

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
