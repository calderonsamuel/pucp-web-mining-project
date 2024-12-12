from shiny import App, reactive, render, ui
import polars as pl
import numpy as np
import faicons

def euclidean_distance(arr1, arr2):
        return np.sqrt(np.sum((np.array(arr1) - np.array(arr2)) ** 2))

# def compute_distance(v1: pl.Series, v2: pl.Series) -> pl.Series:
#     # Apply the function element-wise to the Series
#     return pl.Series([euclidean_distance(a, b) for a, b in zip(v1, v2)])

def compute_distance(v1: pl.Series, v2: pl.Series) -> pl.Series:
    # Ensure v2 is of length 1 and extend it to match v1's length
    v2_extended = [v2[0]] * len(v1)
    
    # Compute distances
    distances = [euclidean_distance(a, b) for a, b in zip(v1, v2_extended)]
    
    return pl.Series('distances', distances)

# ordenes = pl.read_parquet('data/ordenes.parquet')
ordenes = pl.read_parquet('data/ordenes_full.parquet')
ordenes_lda = pl.read_parquet('data/ordenes_lda.parquet')
nombre_rubros = pl.read_csv('data/nombre_rubros.csv')


choices_anno = ordenes['Anno'].unique().sort().to_list()
choices_entidad = ordenes['Entidad'].unique().sort().to_list()
choices_tipo = ordenes['Tipo'].unique().sort().to_list()

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
        ui.input_selectize(
            id="tipo",
            label="Tipo de orden",
            choices=choices_tipo,
            multiple=True,
            selected=choices_tipo
        ),
        ui.input_text(
            id="busqueda",
            label="Búsqueda",
            placeholder="Buscar por descripción..."
        ),
        ui.input_action_button(
            id="reset_busqueda",
            label="Limpiar búsqueda",
            icon=faicons.icon_svg("broom")
        )
    ),
    ui.card(
        ui.layout_columns(
            ui.layout_columns(
                ui.value_box(
                    title="Número de Órdenes",
                    value = ui.output_text("total_ordenes"),
                    showcase=faicons.icon_svg("file-invoice-dollar")
                ),
                ui.value_box(
                    title="Gasto anual",
                    value = ui.output_text("total_gasto"),
                    showcase=faicons.icon_svg("coins")
                ),
                col_widths=12
            ),
            ui.output_data_frame("table"),
            col_widths={
                "sm": [5, 7],
                "lg": [4, 8]
            }
        ),
        max_height="50%"
    ),
    ui.card(
        ui.layout_columns(
            ui.value_box(
                title = ui.output_text("nombre_rubro"),
                value = ui.output_text("palabras_clave")
            ),
            ui.output_data_frame("table_lda"),
            col_widths={
                "sm": [5, 7],
                "lg": [4, 8]
            }
        )
    ),
    title="Explorador de Órdenes de Servicio/Compra",
    fillable=True
)

def server(input, output, session):
    @reactive.calc
    def data_filtered():
        filtered_basic = ordenes.filter(
            pl.col("Anno") == int(input.year()),
            pl.col("Entidad") == input.entidad(),
            pl.col("Tipo").is_in(input.tipo())
        )

        if (input.busqueda() == ""):
            return filtered_basic
        
        filtered_basic = filtered_basic.filter(
            pl.col("Descripcion").str.contains("(?i)" + input.busqueda(), literal=False)
        )

        return filtered_basic
    
    @reactive.calc
    def selected_orden_id():
        selection = input.table_cell_selection()["rows"]

        if len(selection) == 0:
            return ""
        
        index = selection[0]

        return data_filtered().slice(index, 1).drop_in_place("ID")[0] 
    
    @reactive.calc
    def selected_lda():
        if selected_orden_id() == "":
            return None
        
        data_subset = ordenes.filter(
            pl.col("ID") == selected_orden_id()
        ).drop(["ID", "Anno", "Entidad", "Tipo", "Numero", "Mes", "Descripcion", "Monto"])
        
        return data_subset
    
    @reactive.calc
    def selected_vector_probs():
        if selected_lda() is None:
            return None
        
        vector = selected_lda().drop_in_place("vector_probabilidades")
        return vector
    
    @reactive.calc
    def most_similar_orders():
        if selected_vector_probs() is None:
            return None
        
        table = ordenes.with_columns(
            compute_distance(ordenes["vector_probabilidades"], selected_vector_probs()).alias("distancia")
        ).filter(
            pl.col("ID") != selected_orden_id()
        ).sort("distancia").slice(0, 10)
        
        return table

    @render.data_frame
    def table():
        data_to_render = data_filtered().select(
            pl.col("ID"),
            pl.col("Tipo"),
            pl.col("Numero"),
            pl.col("Mes"),
            pl.col("Descripcion"),
            pl.col("Monto")
        )
        return render.DataGrid(data_to_render, selection_mode="row")
    
    @render.text
    def total_ordenes():
        return data_filtered().shape[0]
    
    @render.text
    def total_gasto():
        suma = data_filtered()["Monto"].sum()
        return f"S/. {suma:,.2f}".replace(",", " ")
    
    @render.text
    def otros():
        if selected_orden_id() == "":
            return "Selecciona una orden para ver detalles"
        return None
    
    @render.text
    def nombre_rubro():
        if selected_lda() is None:
            return None
        
        joined = selected_lda().join(nombre_rubros, on="rubro_asignado")
        id_rubro = joined.drop_in_place("rubro_asignado").to_list()[0]
        nombre = joined.drop_in_place("nombre").to_list()[0]
        return f"{id_rubro} - {nombre}"
    
    @render.text
    def palabras_clave():
        if selected_lda() is None:
            return None
        
        palabras = selected_lda().drop_in_place("palabras_clave").to_list()[0]
        return palabras
    
    @render.data_frame
    def table_lda():
        if most_similar_orders() is None:
            return None
        return render.DataGrid(most_similar_orders())
    
    @reactive.effect
    @reactive.event(input.reset_busqueda)
    def event_reset_busqueda():
        ui.update_text(
            id="busqueda",
            value=""
        )
        return None
    
    @reactive.effect
    @reactive.event(selected_vector_probs)
    def event_selected_vector_probs():
        print(selected_vector_probs())
        print(most_similar_orders())
        return None

app = App(app_ui, server)
