use super::model::ReportBundle;
use crate::infra::output::reporting::{ReportOutputArgs, ReportView};
use std::io;

pub type MarkdownWriter =
    Box<dyn for<'a, 'b> Fn(&'a ReportBundle, &'b str) -> io::Result<()> + 'static>;

pub fn emit_report(
    report: &ReportBundle,
    output_args: &ReportOutputArgs,
    default_view: Option<ReportView>,
    quick_view: Option<ReportView>,
    markdown_writer: Option<MarkdownWriter>,
) {
    emit_report_with_extra_json(
        report,
        output_args,
        default_view,
        quick_view,
        markdown_writer,
        None,
    );
}

pub fn emit_report_with_extra_json(
    report: &ReportBundle,
    output_args: &ReportOutputArgs,
    default_view: Option<ReportView>,
    quick_view: Option<ReportView>,
    markdown_writer: Option<MarkdownWriter>,
    extra_json_out: Option<&str>,
) {
    let json_pretty = || report.to_json_pretty();
    if output_args.json_mode {
        println!("{}", json_pretty());
    } else if output_args.tree_mode {
        println!("\n{}", report.render_tree());
    } else if output_args.quick_mode {
        match quick_view {
            Some(ReportView::Tree) => println!("\n{}", report.render_tree()),
            Some(ReportView::Summary) => report.print_summary(),
            None => {}
        }
    } else {
        match default_view {
            Some(ReportView::Tree) => println!("\n{}", report.render_tree()),
            Some(ReportView::Summary) => report.print_summary(),
            None => {}
        }
    }

    if let Some(path) = output_args.json_out.as_deref() {
        report.write_json(path).expect("write JSON");
        eprintln!("JSON written to {path}");
    }
    if let Some(path) = output_args.csv_out.as_deref() {
        report.write_csv(path).expect("write CSV");
        eprintln!("CSV written to {path}");
    }
    if let Some(path) = output_args.markdown_out.as_deref() {
        if let Some(writer) = markdown_writer {
            writer(report, path).expect("write markdown");
        } else {
            report.write_markdown(path).expect("write markdown");
        }
        eprintln!("Markdown written to {path}");
    }
    if let Ok(path) = std::env::var(crate::infra::env::JSON_OUT) {
        report.write_json(&path).expect("write JSON");
    }
    if let Some(path) = extra_json_out {
        report.write_json(path).expect("write extra JSON");
    }
}
