package fr.dechoux.bertrand.cascading.deviation.pipe.assembly;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascalog.MemorySourceTap;
import cascalog.StdoutTap;

public class DeviationByTest {

	@Test
	@SuppressWarnings("unchecked")
	public void shouldComputeAverageAndDeviationFromSinglePipe() {
		// given
		final List<Tuple> tuples = new ArrayList<Tuple>();
		tuples.addAll(wordNums("a", 42));
		tuples.addAll(wordNums("b", 0, 10));
		tuples.addAll(wordNums("c", 2, 4, 4, 4, 5, 5, 7, 9));

		final Tap source = new MemorySourceTap(tuples, new Fields("word", "num"));
		final Tap sink = new StdoutTap();

		Pipe pipe = new Pipe("average and deviation test");
		pipe = new DeviationBy(pipe, new Fields("word"), new Fields("num"), new Fields("average", "deviation"), 2);

		// when
		final Flow flow = new FlowConnector().connect(source, sink, pipe);
		flow.complete();

		// then
		assertThat(flatResultsFrom(flow)).isEqualTo(Arrays.asList("a", 42D, 0D, "b", 5D, 5D, "c", 5D, 2D));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void shouldComputeAverageAndDeviationWhileMergePipes() {
		// given
		final List<Tuple> lhsTuples = new ArrayList<Tuple>();
		lhsTuples.addAll(wordNums("a", 42));
		lhsTuples.addAll(wordNums("b", 10));
		lhsTuples.addAll(wordNums("c", 4, 4, 5, 9));

		final List<Tuple> rhsTuples = new ArrayList<Tuple>();
		rhsTuples.addAll(wordNums("b", 0));
		rhsTuples.addAll(wordNums("c", 2, 4, 5, 7));

		final Tap lhsSource = new MemorySourceTap(lhsTuples, new Fields("word", "num"));
		final Tap rhsSource = new MemorySourceTap(rhsTuples, new Fields("word", "num"));
		final Tap sink = new StdoutTap();

		Pipe lhsPipe = new Pipe("lhs-pipe");
		Pipe rhsPipe = new Pipe("rhs-pipe");

		Map<String, Tap> tapMap = Cascades.tapsMap(Pipe.pipes(lhsPipe, rhsPipe), Tap.taps(lhsSource, rhsSource));

		Pipe pipe = new DeviationBy("average and deviation test", Pipe.pipes(lhsPipe, rhsPipe),//
				new Fields("word"), new Fields("num"), new Fields("average", "deviation"), 2);

		// when
		final Flow flow = new FlowConnector().connect(tapMap, sink, pipe);
		flow.complete();

		// then
		assertThat(flatResultsFrom(flow)).isEqualTo(Arrays.asList("a", 42D, 0D, "b", 5D, 5D, "c", 5D, 2D));
	}

	private Collection<? extends Tuple> wordNums(String word, double... nums) {
		final List<Tuple> tuples = new ArrayList<Tuple>();
		for (double num : nums) {
			tuples.add(new Tuple(word, num));
		}
		return tuples;
	}

	private List<?> flatResultsFrom(final Flow flow) {
		final List<Object> flatResults = new ArrayList<Object>();
		TupleEntryIterator it = null;
		try {
			it = flow.openSink();
			while (it.hasNext()) {
				final Tuple tuple = it.next().getTuple();
				for (Object object : tuple) {
					flatResults.add(object);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (it != null) {
				it.close();
			}
		}
		return flatResults;
	}

}
