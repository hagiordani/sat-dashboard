# ---------------------------------------------------------
# CARGA CSV
# ---------------------------------------------------------
@app.route('/carga_csv', methods=['GET', 'POST'])
def carga_csv():

    import dateutil.parser

    def convertir_fecha(valor):
        """
        Convierte cualquier fecha del SAT a formato MySQL YYYY-MM-DD.
        Si no se puede convertir, regresa None.
        """
        if not valor or str(valor).strip() == "" or str(valor).lower() in ["nan", "null", "-", "--", "—"]:
            return None

        try:
            valor = str(valor).replace("\n", " ").strip()
            fecha = dateutil.parser.parse(valor, dayfirst=True, fuzzy=True)
            return fecha.strftime("%Y-%m-%d")
        except Exception:
            return None

    if request.method == 'POST':

        # ============================
        # Validación inicial del archivo
        # ============================
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

        # ============================
        # Validación de tabla destino
        # ============================
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
            # ============================
            # Leer CSV (encabezados en línea 3)
            # ============================
            df = pd.read_csv(archivo, header=2)

            if df.empty:
                flash('El archivo CSV está vacío', 'danger')
                return redirect(request.url)

            # ============================
            # Mapeos por tabla
            # ============================
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

            # ============================
            # Renombrar columnas según tabla
            # ============================
            df.rename(columns=mapeos.get(tabla_real, {}), inplace=True)

            # ============================
            # Conversión automática de fechas
            # ============================
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

            # ============================
            # Conexión segura
            # ============================
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # ============================
            # Leer texto legal (líneas 1 y 2)
            # ============================
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

            # ============================
            # Validar columnas ANTES de borrar nada
            # ============================
            cursor.execute(f"DESCRIBE {tabla_real}")
            columnas_tabla = [col['Field'] for col in cursor.fetchall()]

            columnas_validas = [c for c in df.columns if c in columnas_tabla]

            if not columnas_validas:
                flash('El CSV no contiene columnas válidas para esta tabla. No se realizaron cambios.', 'danger')
                return redirect(request.url)

            # ============================
            # Crear backup
            # ============================
            fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
            tabla_backup = f"{tabla_real}_backup_{fecha}"

            cursor.execute(f"CREATE TABLE {tabla_backup} AS SELECT * FROM {tabla_real}")
            conn.commit()

            flash(f"✅ Backup creado correctamente: <strong>{tabla_backup}</strong>", "info")

            # ============================
            # Vaciar tabla original
            # ============================
            cursor.execute(f"TRUNCATE TABLE {tabla_real}")
            conn.commit()

            # ============================
            # Insertar datos nuevos
            # ============================
            df = df[columnas_validas]
            df = df.where(pd.notnull(df), None)

            placeholders = ", ".join(["%s"] * len(columnas_validas))
            columnas_sql = ", ".join(columnas_validas)
            query = f"INSERT INTO {tabla_real} ({columnas_sql}) VALUES ({placeholders})"

            registros = df.values.tolist()
            cursor.executemany(query, registros)
            conn.commit()

            total = cursor.rowcount

            # ============================
            # Registrar en historial
            # ============================
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
