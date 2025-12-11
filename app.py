#!/usr/bin/env python3
"""
Sistema SAT - Interfaz Web (Flask)
Versión limpia, sin duplicados, lista para producción.
"""

from flask import Flask, render_template, request, jsonify, flash, redirect, send_file
from config import DB_CONFIG
import mysql.connector
from datetime import datetime
import pandas as pd
import os
import io
import csv
import traceback
import json
import dateutil.parser
from werkzeug.utils import secure_filename

# ---------------------------------------------------------
# CONFIGURACIÓN GENERAL
# ---------------------------------------------------------

app = Flask(__name__)
app.secret_key = 'sat_secret_key_2024'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'txt'}

# ---------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        print(f"Error de base de datos: {e}")
        return None


@app.context_processor
def inject_now():
    return {'now': datetime.now(), 'app_name': 'Sistema SAT'}


def buscar_rfc_en_tablas(rfc, cursor):
    tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
    encontradas = []

    for tabla in tablas:
        try:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {tabla} WHERE UPPER(rfc) = %s", (rfc.upper(),))
            if cursor.fetchone()['count'] > 0:
                encontradas.append(tabla)
        except:
            pass

    return encontradas

# ---------------------------------------------------------
# DASHBOARD PRINCIPAL
# ---------------------------------------------------------

@app.route("/")
def index():
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

        registros_por_tabla = {}
        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {tabla}")
            registros_por_tabla[tabla] = cursor.fetchone()['count']

        tablas_json = {
            "labels": list(registros_por_tabla.keys()),
            "values": list(registros_por_tabla.values())
        }

        cursor.execute("""
            SELECT DATE(fecha) AS dia, COUNT(*) AS total
            FROM Historial_Cargas
            GROUP BY DATE(fecha)
            ORDER BY dia DESC
            LIMIT 7
        """)
        cargas = cursor.fetchall()

        cargas_dias_json = {
            "labels": [str(c["dia"]) for c in cargas][::-1],
            "values": [c["total"] for c in cargas][::-1]
        }

        cursor.execute("""
            SELECT situacion_contribuyente AS situacion, COUNT(*) AS total
            FROM Listado_Completo_69_B
            GROUP BY situacion_contribuyente
            ORDER BY total DESC
        """)
        situaciones = cursor.fetchall()

        estados_json = {
            "labels": [s["situacion"] for s in situaciones],
            "values": [s["total"] for s in situaciones]
        }

        total_registros = sum(registros_por_tabla.values())
        total_tablas = len(tablas)

        cursor.execute("SELECT fecha FROM Historial_Cargas ORDER BY fecha DESC LIMIT 1")
        ultima = cursor.fetchone()
        ultima_carga = ultima["fecha"] if ultima else "N/A"

        cursor.execute("""
            SELECT COUNT(*) AS total
            FROM Historial_Cargas
            WHERE DATE(fecha) = CURDATE()
        """)
        procesados_hoy = cursor.fetchone()["total"]

        cursor.close()
        conn.close()

        return render_template(
            "index.html",
            total_registros=total_registros,
            total_tablas=total_tablas,
            ultima_carga=ultima_carga,
            procesados_hoy=procesados_hoy,
            tablas_json=json.dumps(tablas_json),
            cargas_dias_json=json.dumps(cargas_dias_json),
            estados_json=json.dumps(estados_json)
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500


# ---------------------------------------------------------
# BÚSQUEDA
# ---------------------------------------------------------

@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    search_type = request.args.get('type', 'rfc')

    if not query:
        return render_template('search.html', results=[], query='', search_type=search_type)

    query = query.upper()

    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        results = []
        tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

        if search_type == 'rfc':
            for tabla in tablas:
                cursor.execute(f"""
                    SELECT *, '{tabla}' AS tabla_origen
                    FROM {tabla}
                    WHERE UPPER(rfc) = %s
                    ORDER BY numero
                """, (query,))
                results.extend(cursor.fetchall())
        else:
            for tabla in tablas:
                cursor.execute(f"""
                    SELECT *, '{tabla}' AS tabla_origen
                    FROM {tabla}
                    WHERE UPPER(nombre_contribuyente) LIKE %s
                    ORDER BY numero
                    LIMIT 100
                """, (f"%{query}%",))
                results.extend(cursor.fetchall())

        cursor.close()
        conn.close()

        return render_template(
            'search.html',
            results=results,
            query=query,
            search_type=search_type,
            results_count=len(results)
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500


# ---------------------------------------------------------
# API RFC
# ---------------------------------------------------------

@app.route('/api/contribuyente/<rfc>')
def api_contribuyente(rfc):
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión a la base de datos'}), 500

    cursor = conn.cursor(dictionary=True)
    tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
    results = []

    try:
        for tabla in tablas:
            cursor.execute(f"SELECT * FROM {tabla} WHERE UPPER(rfc) = %s", (rfc.upper(),))
            for row in cursor.fetchall():
                row['tabla_origen'] = tabla
                results.append(row)

        cursor.close()
        conn.close()
        return jsonify(results)

    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------
# ESTADÍSTICAS DETALLADAS
# ---------------------------------------------------------

@app.route('/estadisticas')
def estadisticas():
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

        try:
            cursor.execute("SELECT tabla, linea1, linea2 FROM Texto_Legal_Tablas")
            textos_legales = cursor.fetchall()
        except:
            textos_legales = []

        stats = {}
        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) AS total FROM {tabla}")
            stats[tabla] = cursor.fetchone()['total']

        duplicates = {}
        for tabla in tablas:
            cursor.execute(f"""
                SELECT COUNT(*) AS duplicate_count
                FROM (
                    SELECT rfc, COUNT(*) AS count
                    FROM {tabla}
                    WHERE rfc IS NOT NULL
                    GROUP BY rfc
                    HAVING COUNT(*) > 1
                ) AS dups
            """)
            duplicates[tabla] = cursor.fetchone()['duplicate_count']

        cursor.execute("""
            SELECT situacion_contribuyente, COUNT(*) AS count
            FROM Listado_Completo_69_B
            GROUP BY situacion_contribuyente
            ORDER BY count DESC
        """)
        situaciones = cursor.fetchall()

        cursor.execute("""
            SELECT 
                table_name,
                MAX(fecha_actualizacion) AS ultima_actualizacion,
                COUNT(*) AS total_registros
            FROM (
                SELECT 'Definitivos' AS table_name, fecha_actualizacion FROM Definitivos
                UNION ALL SELECT 'Desvirtuados', fecha_actualizacion FROM Desvirtuados
                UNION ALL SELECT 'Presuntos', fecha_actualizacion FROM Presuntos
                UNION ALL SELECT 'SentenciasFavorables', fecha_actualizacion FROM SentenciasFavorables
                UNION ALL SELECT 'Listado_Completo_69_B', fecha_actualizacion FROM Listado_Completo_69_B
            ) AS all_tables
            GROUP BY table_name
            ORDER BY table_name
        """)
        actualizaciones = cursor.fetchall()

        cursor.close()
        conn.close()

        return render_template(
            'estadisticas.html',
            stats=stats,
            duplicates=duplicates,
            situaciones=situaciones,
            actualizaciones=actualizaciones,
            textos_legales=textos_legales
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500


# ---------------------------------------------------------
# TABLAS
# ---------------------------------------------------------

@app.route('/tablas')
def tablas():
    tablas_info = [
        {'nombre': 'Definitivos', 'ruta': 'definitivos', 'descripcion': 'Contribuyentes con situación definitiva'},
        {'nombre': 'Desvirtuados', 'ruta': 'desvirtuados', 'descripcion': 'Contribuyentes desvirtuados'},
        {'nombre': 'Presuntos', 'ruta': 'presuntos', 'descripcion': 'Contribuyentes presuntos'},
        {'nombre': 'Sentencias Favorables', 'ruta': 'sentenciasfavorables', 'descripcion': 'Sentencias favorables'},
        {'nombre': 'Listado Completo 69-B', 'ruta': 'listado_completo_69_b', 'descripcion': 'Listado completo del artículo 69-B'}
    ]
    return render_template('tablas.html', tablas=tablas_info)


@app.route('/tabla/<nombre_tabla>')
def ver_tabla(nombre_tabla):
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas_validas = {
            'definitivos': 'Definitivos',
            'desvirtuados': 'Desvirtuados',
            'presuntos': 'Presuntos',
            'sentenciasfavorables': 'SentenciasFavorables',
            'listado_completo_69_b': 'Listado_Completo_69_B'
        }

        tabla_real = tablas_validas.get(nombre_tabla.lower())
        if not tabla_real:
            return "Tabla no válida", 400

        page = request.args.get('page', 1, type=int)
        per_page = 50
        offset = (page - 1) * per_page

        cursor.execute(f"SELECT COUNT(*) AS total FROM {tabla_real}")
        total = cursor.fetchone()['total']

        cursor.execute(f"""
            SELECT * FROM {tabla_real}
            ORDER BY numero
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        registros = cursor.fetchall()

        cursor.execute(f"DESCRIBE {tabla_real}")
        columnas = [col['Field'] for col in cursor.fetchall()]

        cursor.execute("""
            SELECT linea1, linea2 
            FROM Texto_Legal_Tablas 
            WHERE tabla = %s 
            ORDER BY id DESC 
            LIMIT 1
        """, (tabla_real,))
        texto_legal = cursor.fetchone()

        total_pages = (total + per_page - 1) // per_page

        tabla_info = {
            'definitivos': {'nombre': 'Definitivos', 'descripcion': 'Contribuyentes con situación definitiva'},
            'desvirtuados': {'nombre': 'Desvirtuados', 'descripcion': 'Contribuyentes desvirtuados'},
            'presuntos': {'nombre': 'Presuntos', 'descripcion': 'Contribuyentes presuntos'},
            'sentenciasfavorables': {'nombre': 'Sentencias Favorables', 'descripcion': 'Sentencias favorables'},
            'listado_completo_69_b': {'nombre': 'Listado Completo 69-B', 'descripcion': 'Listado completo del artículo 69-B'}
        }.get(nombre_tabla.lower())

        cursor.close()
        conn.close()

        return render_template(
            'tabla_detalle.html',
            tabla=tabla_real,
            tabla_info=tabla_info,
            registros=registros,
            columnas=columnas,
            page=page,
            total_pages=total_pages,
            total=total,
            texto_legal=texto_legal
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500


# ---------------------------------------------------------
# EXPORTAR CSV
# ---------------------------------------------------------

@app.route('/exportar/<nombre_tabla>')
def exportar_tabla(nombre_tabla):
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas_validas = {
            'definitivos': 'Definitivos',
            'desvirtuados': 'Desvirtuados',
            'presuntos': 'Presuntos',
            'sentenciasfavorables': 'SentenciasFavorables',
            'listado_completo_69_b': 'Listado_Completo_69_B'
        }

        tabla_real = tablas_validas.get(nombre_tabla.lower())
        if tabla_real is None:
            return "Tabla no válida", 400

        cursor.execute(f"SELECT * FROM {tabla_real} ORDER BY numero")
        registros = cursor.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)

        if registros:
            writer.writerow(registros[0].keys())

        for registro in registros:
            writer.writerow(registro.values())

        output.seek(0)

        cursor.close()
        conn.close()

        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{tabla_real}_{datetime.now().strftime('%Y%m%d')}.csv"
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500


# ---------------------------------------------------------
# CARGA CSV (VERSIÓN FINAL Y CORREGIDA)
# ---------------------------------------------------------

@app.route('/carga_csv', methods=['GET', 'POST'])
def carga_csv():

    def convertir_fecha(valor):
        if not valor or str(valor).strip() == "" or str(valor).lower() in ["nan", "null", "-", "--", "—"]:
            return None
        try:
            valor = str(valor).replace("\n", " ").strip()
            fecha = dateutil.parser.parse(valor, dayfirst=True, fuzzy=True)
            return fecha.strftime("%Y-%m-%d")
        except Exception:
            return None

    if request.method == 'POST':

        # Validación inicial del archivo
        if 'archivo' not in request.files:
            flash('No se seleccionó ningún archivo', 'danger')
            return redirect(request.url)

        archivo = request.files['archivo']

        if archivo.filename == '':
            flash('No se seleccionó ningún archivo', 'danger')
            return redirect(request.url)

        if not archivo.filename.lower().endswith('.csv'):
            flash('Solo se permiten archivos CSV', 'danger')
            return redirect(request.url)

        # Validación de tabla destino
        tabla = request.form.get('tabla')
        tablas_validas = {
            'definitivos': 'Definitivos',
            'desvirtuados': 'Desvirtuados',
            'presuntos': 'Presuntos',
            'sentenciasfavorables': 'SentenciasFavorables',
            'listado_completo_69_b': 'Listado_Completo_69_B'
        }

        tabla_real = tablas_validas.get(tabla.lower())
        if tabla_real is None:
            flash('Tabla destino no válida', 'danger')
            return redirect(request.url)

        conn = None
        cursor = None

        try:
            # Leer CSV (encabezados en línea 3)
            df = pd.read_csv(archivo, header=2)

            if df.empty:
                flash('El archivo CSV está vacío', 'danger')
                return redirect(request.url)

            # Mapeos por tabla
            mapeo_definitivos = {
                "No.": "numero",
                "RFC": "rfc",
                "Nombre del Contribuyente": "nombre_contribuyente",
                "Situación del contribuyente": "situacion_contribuyente",
                "Número y fecha de oficio global de presunción SAT": "oficio_presuncion_sat",
                "Publicación página SAT presuntos": "publicacion_sat_presuntos",
                "Número y fecha de oficio global de presunción DOF": "oficio_presuncion_dof",
                "Publicación DOF presuntos": "publicacion_dof_presuntos",
                "Número y fecha de oficio global de contribuyentes que desvirtuaron SAT": "oficio_desvirtuado_sat",
                "Publicación página SAT desvirtuados": "publicacion_sat_desvirtuados",
                "Número y fecha de oficio global de contribuyentes que desvirtuaron DOF": "oficio_desvirtuado_dof",
                "Publicación DOF desvirtuados": "publicacion_dof_desvirtuados",
                "Número y fecha de oficio global de definitivos SAT": "oficio_definitivo_sat",
                "Publicación página SAT definitivos": "publicacion_sat_definitivos",
                "Número y fecha de oficio global de definitivos DOF": "oficio_definitivo_dof",
                "Publicación DOF definitivos": "publicacion_dof_definitivos",
                "Número y fecha de oficio global de sentencia favorable SAT": "oficio_sentencia_sat",
                "Publicación página SAT sentencia favorable": "publicacion_sat_sentencia",
                "Número y fecha de oficio global de sentencia favorable DOF": "oficio_sentencia_dof",
                "Publicación DOF sentencia favorable": "publicacion_dof_sentencia"
            }

            mapeo_presuntos = {
                "No.": "numero",
                "RFC": "rfc",
                "Nombre del Contribuyente": "nombre_contribuyente",
                "Situación del contribuyente": "situacion_contribuyente",
                "Número y fecha de oficio global de presunción SAT": "oficio_presuncion_sat",
                "Publicación página SAT presuntos": "publicacion_sat_presuntos",
                "Número y fecha de oficio global de presunción DOF": "oficio_presuncion_dof",
                "Publicación DOF presuntos": "publicacion_dof_presuntos"
            }

            mapeo_desvirtuados = {
                "No.": "numero",
                "RFC": "rfc",
                "Nombre del Contribuyente": "nombre_contribuyente",
                "Situación del contribuyente": "situacion_contribuyente",
                "Número y fecha de oficio global de contribuyentes que desvirtuaron SAT": "oficio_desvirtuado_sat",
                "Publicación página SAT desvirtuados": "publicacion_sat_desvirtuados",
                "Número y fecha de oficio global de contribuyentes que desvirtuaron DOF": "oficio_desvirtuado_dof",
                "Publicación DOF desvirtuados": "publicacion_dof_desvirtuados"
            }

            mapeo_sentencias = {
                "No.": "numero",
                "RFC": "rfc",
                "Nombre del Contribuyente": "nombre_contribuyente",
                "Situación del contribuyente": "situacion_contribuyente",
                "Número y fecha de oficio global de sentencia favorable SAT": "oficio_sentencia_sat",
                "Publicación página SAT sentencia favorable": "publicacion_sat_sentencia",
                "Número y fecha de oficio global de sentencia favorable DOF": "oficio_sentencia_dof",
                "Publicación DOF sentencia favorable": "publicacion_dof_sentencia"
            }

            mapeo_listado_completo = {
                "No.": "numero",
                "RFC": "rfc",
                "Nombre del Contribuyente": "nombre_contribuyente",
                "Situación del contribuyente": "situacion_contribuyente",
                "Número y fecha de oficio global de presunción SAT": "oficio_presuncion_sat",
                "Publicación página SAT presuntos": "publicacion_sat_presuntos",
                "Número y fecha de oficio global de presunción DOF": "oficio_presuncion_dof",
                "Publicación DOF presuntos": "publicacion_dof_presuntos",
                "Número y fecha de oficio global de contribuyentes que desvirtuaron SAT": "oficio_desvirtuado_sat",
                "Publicación página SAT desvirtuados": "publicacion_sat_desvirtuados",
                "Número y fecha de oficio global de contribuyentes que desvirtuaron DOF": "oficio_desvirtuado_dof",
                "Publicación DOF desvirtuados": "publicacion_dof_desvirtuados",
                "Número y fecha de oficio global de definitivos SAT": "oficio_definitivo_sat",
                "Publicación página SAT definitivos": "publicacion_sat_definitivos",
                "Número y fecha de oficio global de definitivos DOF": "oficio_definitivo_dof",
                "Publicación DOF definitivos": "publicacion_dof_definitivos",
                "Número y fecha de oficio global de sentencia favorable SAT": "oficio_sentencia_sat",
                "Publicación página SAT sentencia favorable": "publicacion_sat_sentencia",
                "Número y fecha de oficio global de sentencia favorable DOF": "oficio_sentencia_dof",
                "Publicación DOF sentencia favorable": "publicacion_dof_sentencia"
            }

            mapeos = {
                "Definitivos": mapeo_definitivos,
                "Presuntos": mapeo_presuntos,
                "Desvirtuados": mapeo_desvirtuados,
                "SentenciasFavorables": mapeo_sentencias,
                "Listado_Completo_69_B": mapeo_listado_completo
            }

            # Renombrar columnas según tabla
            df.rename(columns=mapeos.get(tabla_real, {}), inplace=True)

            # Conversión automática de fechas
            columnas_fecha = [
                "publicacion_sat_presuntos",
                "publicacion_dof_presuntos",
                "publicacion_sat_desvirtuados",
                "publicacion_dof_desvirtuados",
                "publicacion_sat_definitivos",
                "publicacion_dof_definitivos",
                "publicacion_sat_sentencia",
                "publicacion_dof_sentencia",
                "fecha_actualizacion"
            ]

            for col in columnas_fecha:
                if col in df.columns:
                    df[col] = df[col].apply(convertir_fecha)

            # Conexión segura
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # Leer texto legal (líneas 1 y 2)
            archivo.stream.seek(0)
            lineas = archivo.stream.read().decode('latin1').splitlines()
            linea1 = lineas[0] if len(lineas) > 0 else ""
            linea2 = lineas[1] if len(lineas) > 1 else ""

            cursor.execute("DELETE FROM Texto_Legal_Tablas WHERE tabla = %s", (tabla_real,))
            cursor.execute("""
                INSERT INTO Texto_Legal_Tablas (tabla, linea1, linea2)
                VALUES (%s, %s, %s)
            """, (tabla_real, linea1, linea2))
            conn.commit()

            # Validar columnas ANTES de borrar nada
            cursor.execute(f"DESCRIBE {tabla_real}")
            columnas_tabla = [col['Field'] for col in cursor.fetchall()]

            columnas_validas = [c for c in df.columns if c in columnas_tabla]

            if not columnas_validas:
                flash('El CSV no contiene columnas válidas para esta tabla. No se realizaron cambios.', 'danger')
                return redirect(request.url)

            # Crear backup
            fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
            tabla_backup = f"{tabla_real}_backup_{fecha}"

            cursor.execute(f"CREATE TABLE {tabla_backup} AS SELECT * FROM {tabla_real}")
            conn.commit()

            flash(f"✅ Backup creado correctamente: <strong>{tabla_backup}</strong>", "info")

            # Vaciar tabla original
            cursor.execute(f"TRUNCATE TABLE {tabla_real}")
            conn.commit()

            # Insertar datos nuevos
            df = df[columnas_validas]
            df = df.where(pd.notnull(df), None)

            placeholders = ", ".join(["%s"] * len(columnas_validas))
            columnas_sql = ", ".join(columnas_validas)
            query = f"INSERT INTO {tabla_real} ({columnas_sql}) VALUES ({placeholders})"

            registros = df.values.tolist()
            cursor.executemany(query, registros)
            conn.commit()

            total = cursor.rowcount

            # Registrar en historial
            cursor.execute("""
                INSERT INTO Historial_Cargas (nombre_archivo, tabla, registros)
                VALUES (%s, %s, %s)
            """, (archivo.filename, tabla_real, total))
            conn.commit()

            flash(
                f"✅ Tabla {tabla_real} reemplazada correctamente<br>"
                f"✅ Registros cargados: {total}",
                "success"
            )
            return redirect(request.url)

        except Exception as e:
            traceback.print_exc()
            flash(f"Error procesando el archivo: {str(e)}", "danger")
            return redirect(request.url)

        finally:
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            except:
                pass

    return render_template('carga_csv.html')


# ---------------------------------------------------------
# CARGA MASIVA (VISTA DASHBOARD)
# ---------------------------------------------------------

@app.route('/carga_masiva', methods=['GET', 'POST'])
def carga_masiva():
    resultados = []
    total_rfcs = 0
    encontrados = 0
    no_encontrados = 0

    if request.method == 'POST':
        archivo = request.files.get('archivo')

        if not archivo or archivo.filename == '':
            flash('No seleccionaste ningún archivo TXT', 'danger')
            return redirect('/carga_masiva')

        try:
            contenido = archivo.read().decode('latin1').splitlines()
            rfcs = [line.strip().upper() for line in contenido if line.strip()]
            total_rfcs = len(rfcs)

            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

            for rfc in rfcs:
                encontrado = False
                tablas_encontradas = []

                for tabla in tablas:
                    cursor.execute(f"SELECT COUNT(*) AS total FROM {tabla} WHERE UPPER(rfc) = %s", (rfc,))
                    if cursor.fetchone()['total'] > 0:
                        encontrado = True
                        tablas_encontradas.append(tabla)

                resultados.append({
                    'rfc': rfc,
                    'encontrado': encontrado,
                    'tablas': tablas_encontradas
                })

                if encontrado:
                    encontrados += 1
                else:
                    no_encontrados += 1

            cursor.close()
            conn.close()

        except Exception as e:
            traceback.print_exc()
            flash(f"Error procesando el archivo: {str(e)}", 'danger')
            return redirect('/carga_masiva')

    return render_template(
        'carga_masiva.html',
        resultados=resultados,
        total_rfcs=total_rfcs,
        encontrados=encontrados,
        no_encontrados=no_encontrados
    )


# ---------------------------------------------------------
# HISTORIAL DE CARGAS
# ---------------------------------------------------------

@app.route('/historial_cargas')
def historial_cargas():
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT * FROM Historial_Cargas ORDER BY fecha DESC LIMIT 200")
        cargas = cursor.fetchall()
        return render_template('historial_cargas.html', cargas=cargas)

    except Exception as e:
        traceback.print_exc()
        return f"Error: {e}", 500

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


# ---------------------------------------------------------
# DESCARGAR CSV DESDE CARGA MASIVA
# ---------------------------------------------------------

@app.route('/descargar_csv', methods=['POST'])
def descargar_csv():
    archivo = request.files.get('archivo')

    if not archivo or archivo.filename == '':
        flash('No seleccionaste ningún archivo TXT', 'danger')
        return redirect('/carga_masiva')

    conn = None
    cursor = None

    try:
        contenido = archivo.read().decode('latin1').splitlines()
        rfcs = [line.strip().upper() for line in contenido if line.strip()]

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

        resultados = []

        for rfc in rfcs:
            encontrado = False
            tablas_encontradas = []

            for tabla in tablas:
                cursor.execute(f"SELECT COUNT(*) AS total FROM {tabla} WHERE UPPER(rfc) = %s", (rfc,))
                if cursor.fetchone()['total'] > 0:
                    encontrado = True
                    tablas_encontradas.append(tabla)

            resultados.append({
                'rfc': rfc,
                'encontrado': 'SI' if encontrado else 'NO',
                'tablas': ", ".join(tablas_encontradas) if tablas_encontradas else ''
            })

        # Crear CSV en memoria
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['RFC', 'Encontrado', 'Tablas'])

        for r in resultados:
            writer.writerow([r['rfc'], r['encontrado'], r['tablas']])

        output.seek(0)

        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"resultado_carga_masiva_{datetime.now().strftime('%Y%m%d')}.csv"
        )

    except Exception as e:
        traceback.print_exc()
        flash(f"Error generando CSV: {str(e)}", 'danger')
        return redirect('/carga_masiva')

    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass
# ---------------------------------------------------------
# BACKUPS - LISTADO DE TABLAS BACKUP
# ---------------------------------------------------------

@app.route('/backups')
def backups():
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        # Buscar tablas que sean backups
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name LIKE '%_backup_%'
            ORDER BY table_name DESC
        """)
        tablas = cursor.fetchall()

        cursor.close()
        conn.close()

        return render_template('backups.html', tablas=tablas)

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500


# ---------------------------------------------------------
# PUNTO DE ENTRADA (OPCIONAL EN GUNICORN)
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
