# MiT-Manual
Documentation for MiT (CUBRID Migration Toolkit For Graph)

1. Extension of original files of this manual 

   ```
    *.rst
    *.inc
   ```

2. Precaution to edit files.

    Please don't use tab character. Change your editor setting that "change tab as spaces". 1 tab should be 4 spaces.
    
3. How to build manual

   1. Install sphinx package and theme
      1. Install python
      1. Install pip by running command:

         ```
         python get-pip.py
         ```

      1. Install sphinx

         ```
         pip install sphinx
         ```

      1. Install read the docs theme

         ```
         pip install sphinx_rtd_theme
         ```

   1. go to the manual directory (cd ko for Korean; or cd en for English)
   1. make html
   1. now you can see the built html files from _built/

4. Make commands

    ```
    make clean
    ```

    ```
    make html
    ```

5. Mainly used tags on sphinx document(.rst file).

   Basically, indent is very important when you use tags. For details, see http://sphinx-doc.org/

    1. Titles
    
    ```
    *****
    First
    *****
    
    Second
    ======
    
    Third
    -----
    
    Fourth
    ^^^^^^
    
    Fifth
    +++++
    ```
    
    2. bold & italic
    
    ```
    **bold text**
    
    *italic text*
    ```
    
    3. escaping with \
    
    ```
    *italic text*\_  will be "italic text_" : blank is ignored.
    ```
    
    4. dot list
    
    ```
    *   text
    -   text
    ```
    
    5. boxes
    
    ```
    ::
    
        text
    ```
    
    ```
    .. code-block:: sql
    
        SELECT 1;
    ```
    
    ```
    .. note::
    
        note this.
    ```
    
    ```
    .. warning::
    
        warning this.
    ```
    
    6. footnote
    
    ```
    .. rubric:: Footnotes
    
    .. [#f1] word: explanation
    ```
        
    7. function
    
    ```
    .. function:: function_name (arg1, arg2)
    ```
    
    8. option
    
    ```
    .. program:: program_name
    
    .. option:: option_description
    
        explanation
    ```
    
    9. links
    
        1. function link
    
           ```
               :func:`func_name`
           ```
    
        1. footnote link
    
           ```
               [#f1]_
           ```
    
        1. internal link
    
           ```
               .. _link_position:
    
               :ref:`link_positon` or :ref:`title <link_positon>`
        
               :doc:`doc_path_name`
           ```
    
        1. external link
    
           ```
               `title <http address>`_
           ```    
