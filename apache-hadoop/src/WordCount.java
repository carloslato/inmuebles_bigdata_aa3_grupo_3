import java.io.IOException;
import java.util.StringTokenizer;

// Configuración básica de Hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// Tipos de datos de Hadoop
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// Clases de MapReduce
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// Manejo de entrada y salida
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
PROGRAMA: WordCount (Conteo de palabras en Hadoop)

Este programa cuenta cuántas veces aparece cada palabra
en un archivo de texto usando el modelo MapReduce.

FLUJO:

    Mapper → Divide el texto en palabras y emite (palabra, 1)
    Reducer → Suma los valores de cada palabra
    Resultado → (palabra, total)

====================================================
*/

public class WordCount {

/*
============================================
MAPPER
============================================

Entrada:
- key → posición de la línea (no se usa)
- value → línea de texto

Salida:
- (palabra, 1)
*/

public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

    // Valor constante 1
    private final static IntWritable one = new IntWritable(1);

    // Variable para almacenar la palabra
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Convierte la línea en texto
        String line = value.toString();

        // Divide la línea en palabras
        StringTokenizer itr = new StringTokenizer(line);

        // Recorre cada palabra
        while (itr.hasMoreTokens()) {

            // Obtiene palabra
            word.set(itr.nextToken());

            // Emite (palabra, 1)
            context.write(word, one);
        }
    }
}

/*
============================================
REDUCER
============================================

Entrada:
- key → palabra
- values → lista de valores (1,1,1,...)

Salida:
- (palabra, total)
*/

public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

        int sum = 0;

        // Suma todos los valores
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Guarda el resultado
        result.set(sum);

        // Emite (palabra, total)
        context.write(key, result);
    }
}

/*
============================================
MAIN (PUNTO DE ENTRADA)
============================================

Aquí se configura el trabajo de Hadoop
*/

public static void main(String[] args) throws Exception {

    // Configuración del sistema
    Configuration conf = new Configuration();

    // Crear el job
    Job job = Job.getInstance(conf, "Word Count");

    // Clase principal
    job.setJarByClass(WordCount.class);

    // Mapper y Reducer
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    // Tipos de salida
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Rutas de entrada y salida en HDFS
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Ejecutar job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}