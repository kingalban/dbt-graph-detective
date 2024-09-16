
from pathlib import Path
import argparse
import sys


from detective.manifest_helper import ManifestSearcher, Predicate
from detective import predicates


def show_available():
    print("Named predicates:", file=sys.stderr)
    for pred_name in predicates.NAMED_PREDICATES:
        print(f"\t{pred_name}")

    print("Named searches", file=sys.stderr)
    for search_name in predicates.NAMED_SEARCHES:
        print(f"\t{search_name}")


def search(manifest_searcher: ManifestSearcher, predicate: list[str], *, error_on_match=False) -> int:
    predicate_funcs: list[Predicate] = []
    for pred_name in predicate:
        func = predicates.NAMED_PREDICATES.get(pred_name) or predicates.NAMED_SEARCHES.get(pred_name)
        if func is None:
            raise ValueError(f"predicate {pred_name!r} not defined")
        predicate_funcs.append(func)

    results = manifest_searcher.search(*predicate_funcs)

    found_results = False
    for node in results:
        found_results = True
        print(node.name)

    return 1 if found_results and error_on_match else 0


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name")

    search_parser = subparsers.add_parser("search")
    search_parser.add_argument("--dbt-project-dir", default=".",
                               help="path to the manifest file. Mutually exclusive with --dbt-project-dir")
    search_parser.add_argument("--manifest", default=None, type=str,
                               help="path to a manifest.json file")
    search_parser.add_argument("-p", "--predicate", nargs="+", default=tuple(),
                               help="name of a predefined predicate to filter with")
    search_parser.add_argument("--error-on-match", action="store_true",
                               help="set exit code 1 if any results are found")

    show_available_parser = subparsers.add_parser("show-available") # noqa: F841

    args = parser.parse_args()

    if args.subparser_name == "show-available":
        show_available()

    elif args.subparser_name == "search":
        if args.manifest is not None:
            manifest_path = Path(args.manifest)
        else:
            manifest_path = Path(args.dbt_project_dir) / "target" / "manifest.json"

        if not manifest_path.exists():
            raise FileNotFoundError(f"Could not find manifest file at {manifest_path!s}")

        manifest_searcher = ManifestSearcher(str(manifest_path))
        return search(manifest_searcher, args.predicate, error_on_match= args.error_on_match)

    else:
        parser.print_help()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
