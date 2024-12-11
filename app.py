from shiny import App, reactive, render, ui
import polars as pl
import faicons

ordenes = pl.read_parquet('data/ordenes.parquet')

choices_anno = ordenes['Anno'].unique().sort().to_list()
choices_entidad = ordenes['Entidad'].unique().sort().to_list()

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_select(
            id="year",
            label="Año",
            choices=choices_anno
        ),
        ui.input_select(
            id="entidad",
            label="Entidad",
            choices=choices_entidad
        ),
        ui.input_text(
            id="busqueda",
            label="Búsqueda",
            placeholder="Buscar por descripción..."
        )
    ),
    ui.layout_columns(
        ui.value_box(
            title="Órdenes de servicio",
            value = ui.output_text("total_ordenes"),
            showcase=faicons.icon_svg("file-invoice-dollar")
        ),
        ui.value_box(
            title="Gasto total",
            value = ui.output_text("total_gasto"),
            showcase=faicons.icon_svg("coins")
        ),
        ui.value_box(
            title="Gasto total",
            value = 1000
        )            
    ),
    ui.layout_columns(
        ui.card(
            ui.card_header("Tabla"),
            ui.output_data_frame("table")
        )
    ),
    title="Dashboard",
    fillable=True
)

def server(input, output, session):
    @reactive.calc
    def data_filtered():
        filtered_basic = ordenes.filter(
            pl.col("Anno") == input.year(),
            pl.col("Entidad") == input.entidad()
        )

        if (input.busqueda() == ""):
            return filtered_basic
        
        filtered_basic = filtered_basic.filter(
            pl.col("Descripcion").str.contains(input.busqueda(), literal=False)
        )

        return filtered_basic

    @render.data_frame
    def table():
        return data_filtered()
    
    @render.text
    def total_ordenes():
        return data_filtered().shape[0]
    
    @render.text
    def total_gasto():
        suma = data_filtered()["Monto"].cast(pl.Float64).sum()
        return f"S/. {suma:,.2f}".replace(",", " ")

app = App(app_ui, server)
