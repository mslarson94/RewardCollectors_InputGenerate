import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd

def launch_interactive_dashboard(timeline_df):
    app = dash.Dash(__name__)
    unique_ids = timeline_df["cascade_id"].dropna().unique()

    app.layout = html.Div([
        html.H1("Interactive Event Cascade Viewer"),
        dcc.Dropdown(
            id="cascade-id-dropdown",
            options=[{"label": str(int(i)), "value": i} for i in unique_ids],
            value=unique_ids[0],
            clearable=False,
        ),
        dcc.Graph(id="timeline-plot")
    ])

    @app.callback(
        dash.dependencies.Output("timeline-plot", "figure"),
        [dash.dependencies.Input("cascade-id-dropdown", "value")]
    )
    def update_figure(selected_id):
        filtered_df = timeline_df[timeline_df["cascade_id"] == selected_id].sort_values("AppTime")
        fig = px.scatter(
            filtered_df, x="AppTime", y=["event_type"],
            color="event_type", hover_data=["Timestamp", "details"],
            title=f"Cascade {int(selected_id)} Timeline",
        )
        return fig

    app.run_server(debug=True)
