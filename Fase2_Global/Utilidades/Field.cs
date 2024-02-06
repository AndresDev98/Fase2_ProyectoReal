using System;
using System.Collections.Generic;
using System.Text;

namespace Fase2_Global
{
    public class Field
    {
        public string Code { get; set; }
        public int Decimal { get; set; }
        public int Length { get; set; }
        public string Name { get; set; }
        public string Notes { get; set; }
        public bool Required { get; set; }
        public FieldType Type { get; set; }
    }
}