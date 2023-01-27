import argparse
from engines.beam.beam import BeamLoader
from engines.panda.panda import PandaLoader

engine_map = {
    'beam': [BeamLoader],
    'pd': [PandaLoader]
}

ALL_ENGINES = [PandaLoader, BeamLoader]

def get_engine_classes(engine):
    if engine and engine_map.get(engine, None):
        return engine_map.get(engine, [])
    return ALL_ENGINES

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', nargs=2)
    parser.add_argument('-o', '--output')
    parser.add_argument('-e', '--engine', help='Use the specified engine (beam/pd) otherwise all engines are used')
    parser.add_argument('-l', '--lineterm', help='Sets the lineterm to be utilized')
    args = parser.parse_args()
    engine = args.engine or None
    input_files = args.input
    output_file = args.output or 'output.csv'
    lineterm = args.lineterm or '\r'
    for eng_class in get_engine_classes(engine):
        eng = eng_class(
            input_files=input_files,
            output_file=output_file,
            lineterm=lineterm
        )
        eng.run()

if __name__ == '__main__':
    main()