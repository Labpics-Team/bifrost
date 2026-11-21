import IncidentsView from "../views/incidentsView";

// The incident list lays out its own full-height shell (filter sidebar beside the table), as the dashboard does.
export default function AdaptiveRoutingIncidentsPage() {
	return <IncidentsView />;
}
