using System;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Represents a nullable boolean using Three-Valued Logic (Kleene logic).
    /// </summary>
    public readonly struct KleeneBool
    {
        // Internal representation: 0 = False, 1 = True, 2 = NA
        private readonly byte _value;

        private KleeneBool(byte value) => _value = value;

        public static readonly KleeneBool False = new KleeneBool(0);
        public static readonly KleeneBool True = new KleeneBool(1);
        public static readonly KleeneBool NA = new KleeneBool(2);

        public bool IsTrue => _value == 1;
        public bool IsFalse => _value == 0;
        public bool IsNA => _value == 2;

        public static KleeneBool operator &(KleeneBool left, KleeneBool right)
        {
            if (left.IsFalse || right.IsFalse) return False;
            if (left.IsTrue && right.IsTrue) return True;
            return NA;
        }

        public static KleeneBool operator |(KleeneBool left, KleeneBool right)
        {
            if (left.IsTrue || right.IsTrue) return True;
            if (left.IsFalse && right.IsFalse) return False;
            return NA;
        }

        public static KleeneBool operator !(KleeneBool value)
        {
            if (value.IsTrue) return False;
            if (value.IsFalse) return True;
            return NA;
        }

        public override string ToString()
        {
            return _value switch
            {
                0 => "False",
                1 => "True",
                2 => "NA",
                _ => "Unknown"
            };
        }
    }
}
