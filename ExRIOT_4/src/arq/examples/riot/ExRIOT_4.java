package arq.examples.riot;

import org.apache.jena.atlas.lib.Sink ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.out.SinkTripleOutput ;
import org.apache.jena.riot.system.StreamRDF ;
import org.apache.jena.riot.system.StreamRDFBase ;
import org.apache.jena.riot.system.SyntaxLabels ;

import com.hp.hpl.jena.graph.Node ;
import com.hp.hpl.jena.graph.Triple ;
import com.hp.hpl.jena.rdf.model.Property ;
import com.hp.hpl.jena.sparql.vocabulary.FOAF ;

/** Example of using RIOT : extract only certain properties from a parser run */ 
public class ExRIOT_4
{
    public static void main(String...argv)
    {
        String filename = "data.ttl" ;

        // This is the heart of N-triples printing ... output is heavily buffered
        // so the FilterSinkRDF called flush at the end of parsing.
        Sink <Triple> output = new SinkTripleOutput(System.out, null, SyntaxLabels.createNodeToLabel()) ;
        StreamRDF filtered = new FilterSinkRDF(output, FOAF.name, FOAF.knows) ;
        
        // Call the parsing process. 
        RDFDataMgr.parse(filtered, filename) ;
    }
    
    static class FilterSinkRDF extends StreamRDFBase
    {
        private final Node[] properties ;
        // Where to send the filtered triples.
        private final Sink<Triple> dest ;

        FilterSinkRDF(Sink<Triple> dest, Property... properties)
        {
            this.dest = dest ;
            this.properties = new Node[properties.length] ;
            for ( int i = 0 ; i < properties.length ; i++ ) 
                this.properties[i] = properties[i].asNode() ;
        }

        @Override
        public void triple(Triple triple)
        {
            for ( Node p : properties )
            {
                if ( triple.getPredicate().equals(p) )
                    dest.send(triple) ;
            }
        }
        
        @Override
        public void finish()
        {
            // Output may be buffered.
            dest.flush() ;
        }
    }
}
 