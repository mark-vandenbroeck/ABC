import markdown
import os

def convert_md_to_html(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        text = f.read()

    # Extensions for tables, fenced code, etc.
    html_content = markdown.markdown(text, extensions=['extra', 'codehilite', 'tables', 'toc'])

    # Basic GitHub-like CSS
    css = """
    <style>
        body {
            font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
            line-height: 1.6;
            max-width: 800px;
            margin: 0 auto;
            padding: 2rem;
            color: #24292e;
        }
        pre {
            background-color: #f6f8fa;
            padding: 16px;
            border-radius: 6px;
            overflow: auto;
        }
        code {
            font-family: SFMono-Regular,Consolas,"Liberation Mono",Menlo,monospace;
            font-size: 85%;
            padding: 0.2em 0.4em;
            margin: 0;
            background-color: rgba(27,31,35,0.05);
            border-radius: 3px;
        }
        pre code {
            background-color: transparent;
            padding: 0;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 1rem;
        }
        th, td {
            border: 1px solid #dfe2e5;
            padding: 6px 13px;
        }
        th {
            background-color: #f6f8fa;
        }
        blockquote {
            border-left: 0.25em solid #dfe2e5;
            color: #6a737d;
            padding: 0 1em;
            margin: 0;
        }
        img {
            max-width: 100%;
        }
        a {
            color: #0366d6;
            text-decoration: none;
        }
        h1, h2, h3 {
            border-bottom: 1px solid #eaecef;
            padding-bottom: 0.3em;
        }
    </style>
    """

    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>README</title>
        {css}
    </head>
    <body>
        {html_content}
    </body>
    </html>
    """

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(full_html)
    print(f"converted {input_file} to {output_file}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_md = os.path.join(base_dir, 'README.md')
    output_html = os.path.join(base_dir, 'README_print.html')
    convert_md_to_html(input_md, output_html)
