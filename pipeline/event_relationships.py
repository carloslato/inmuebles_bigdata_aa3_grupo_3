PROPERTY_LIFECYCLE = [
    "nueva_propiedad",
    "consulta_usuario",
    "cambio_precio",
    "propiedad_destacada",
    "propiedad_vendida"
]


class PropertyEventTracker:

    def __init__(self):

        self.property_states = {}

    def get_next_event(self, property_id):

        if property_id not in self.property_states:
            self.property_states[property_id] = 0
            return PROPERTY_LIFECYCLE[0]

        current_index = self.property_states[property_id]

        if current_index < len(PROPERTY_LIFECYCLE) - 1:
            current_index += 1

        self.property_states[property_id] = current_index

        return PROPERTY_LIFECYCLE[current_index]

    def generate_realistic_event(self, property_id):

        return self.get_next_event(property_id)
