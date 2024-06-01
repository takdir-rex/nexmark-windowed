# Nexmark Windowed

Improve window implementation in Nexmark queries to avoid continuous incremental state size.

Originally forked from https://github.com/nexmark/nexmark

# Improvement:
* Add a window frame in each input table
* Constant snapshot size during execution
* Add a constant random seed value for the input generator to guarantee reproducibility for experiments
* Add dynamic parameter to assign snapshot regions and slot-sharing groups to each operator. Reference: https://github.com/takdir-rex/regsnap-plus 
