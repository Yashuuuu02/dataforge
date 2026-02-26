import sys
import os

try:
    from pipeline.modes.finetune.formatter import FinetuneFormatterStep
    print("Successfully imported Formatter!")
    
    # Run a quick check
    step = FinetuneFormatterStep()
    print("Instantiated Formatter!")

except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
