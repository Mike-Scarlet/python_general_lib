

If you create a `.gitattributes` file, you can run a filter over certain files before they are added to git. This will leave the original file on disk as-is, but commit the "cleaned" version. 

For this to work, add this to your local `.git/config` or global `~/.gitconfig`:

```git
[filter "strip-notebook-output"]
    clean = "jupyter nbconvert --ClearOutputPreprocessor.enabled=True --to=notebook --stdin --stdout --log-level=ERROR"
```

Then create a `.gitattributes` file in your directory with notebooks, with this content:

```git
*.ipynb filter=strip-notebook-output
```

How this works:
- The attribute tells git to run the filter's `clean` action on each notebook file before adding it to the index (staging).
- The filter is our friend `nbconvert`, set up to read from stdin, write to stdout, strip the output, and only speak when it has something important to say.
- When a file is extracted from the index, the filter's `smudge` action is run, but this is a no-op as we did not specify it. You could run your notebook here to re-create the output (`nbconvert --execute`).
- Note that if the filter somehow fails, the file will be staged unconverted.

My only minor gripe with this process is that I can commit `.gitattributes` but I have to tell my co-workers to update their `.git/config`. 

If you want a hackier but much faster version, try JQ:

```
clean = "jq '.cells[].outputs = [] | .cells[].execution_count = null | .'"
```