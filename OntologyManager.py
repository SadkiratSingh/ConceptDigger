from owlready2 import *
from utilities.constants import home_dir
import types


class Ontology_Manager:
    prefix_dir = f"{home_dir}/wikipedia-digger/ConceptDigger/data-store"
    onto_path.append(prefix_dir)
    default_world.set_backend(filename=f"{prefix_dir}/quadstore.sqlite3")
    ontology_iri = ""
    ontology_file_name = ""
    ontology = None

    @staticmethod
    def commit_changes_to_quadstore():
        default_world.save()
    
    @staticmethod
    def saving_ontology_to_files():
        Ontology_Manager.ontology.save()

    @staticmethod
    def set_ontology_name(name:str):
        prefix_iri = "https://protege.smarter.codes"
        Ontology_Manager.ontology_iri = f"{prefix_iri}/{name}"
        Ontology_Manager.ontology_file_name = name
    
    @staticmethod
    def setAnnotationProperties(props_tuple):
        with Ontology_Manager.ontology:
            for class_name in props_tuple:
                new_annotation_prop = types.new_class(class_name, (AnnotationProperty,))
    
    @staticmethod
    def setDataProperties(props_tuple):
        with Ontology_Manager.ontology:
            for class_name in props_tuple:
                new_data_prop = types.new_class(class_name, (DataProperty,))
    
    @staticmethod
    def load_ontology(create_new:bool):
        if(create_new):
            Ontology_Manager.ontology = get_ontology(Ontology_Manager.ontology_iri)
        else:
            Ontology_Manager.ontology = get_ontology(Ontology_Manager.ontology_iri).load(only_local=True)
        Ontology_Manager.setAnnotationProperties(("has_uri", "has_synonym_source"))
        Ontology_Manager.setDataProperties(("has_synonym",))
    
    @staticmethod
    def add_class_pair(child_class:str, parent_class=Thing):
        with Ontology_Manager.ontology:
            new_class = types.new_class(child_class, (parent_class,))
            return new_class

    @staticmethod
    def delete_ontology():
        onto = get_ontology(Ontology_Manager.ontology_iri)
        onto.destroy()