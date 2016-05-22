package de.unikn.gabriel.mapfilter;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class MapFilterNodeFactory extends NodeFactory<MapFilterNodeModel> {

	@Override
	public MapFilterNodeModel createNodeModel() {
		return new MapFilterNodeModel();
	}

	@Override
	protected int getNrNodeViews() {
		return 0;
	}

	@Override
	public NodeView<MapFilterNodeModel> createNodeView(int viewIndex, MapFilterNodeModel nodeModel) {
		return null;
	}

	@Override
	protected boolean hasDialog() {
		return true;
	}

	@Override
	protected NodeDialogPane createNodeDialogPane() {
		return new MapFilterNodeDialog();
	}

}
