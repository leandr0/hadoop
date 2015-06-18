package enemMedia;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MediaEnemMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	
	private static final int UF_POS 	= 178;
	private static final int ANO_POS 	= 13;
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
		String linha 	= value.toString();
		String uf 		= linha.substring(UF_POS - 1	, UF_POS + 1).trim();
		String ano		= linha.substring(ANO_POS - 1	, ANO_POS + 3).trim();
		
		if ((ano.equals("2011") || ano.equals("2010"))) {
			float mediaNota = getMedia(linha);
			if (mediaNota != -1f)
				context.write(new Text(uf + "/" + ano), new FloatWritable(
						mediaNota));
		}
		
	}

	private float getMedia(final String linha ) {
		
		if (linha.trim().length() < 563 + 8)
			return -1f;

		String presenteCN = "";
		String presenteCH = "";
		String presenteLC = "";
		String presenteMT = "";

		String notaCN = "";
		String notaCH = "";
		String notaLC = "";
		String notaMT = "";

		presenteCN = linha.substring(533 - 1, 533);
		presenteCH = linha.substring(534 - 1, 534);
		presenteLC = linha.substring(535 - 1, 535);
		presenteMT = linha.substring(536 - 1, 536);

		notaCN = linha.substring(537 - 1, 537 + 8);
		notaCH = linha.substring(546 - 1, 546 + 8);	
		notaLC = linha.substring(555 - 1, 555 + 8);
		notaMT = linha.substring(564 - 1, 564 + 8);

		BigDecimal media = BigDecimal.ZERO;
		
		if (presenteCN.equals("1") && presenteCH.equals("1")
				&& presenteLC.equals("1") && presenteMT.equals("1")) {
			media = new BigDecimal(notaCN.trim()).add(
					new BigDecimal(notaCH.trim()).add(new BigDecimal(notaLC
							.trim()).add(new BigDecimal(notaMT.trim()))))
					.divide(new BigDecimal(4));
			return media.floatValue();
		} else
			// não está presente nas provas
			return -1;
	}
}