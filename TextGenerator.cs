using System;
using System.Collections.Generic;
using System.IO;

namespace ManticorePusher
{
    public class TextGenerator
    {
        private readonly List<string> _words = new();
        private readonly Random _rnd = new();

        public TextGenerator(string pathToWords)
        {
            using var fs = File.OpenRead(pathToWords);
            using var rdr = new StreamReader(fs);

            while (!rdr.EndOfStream)
            {
                var txt = rdr.ReadLine();
                if (txt != null)
                    _words.Add(txt);
            }
        }


        public string GetText(int minSize)
        {
            return string.Create(minSize, this, (span, generator) =>
            {
                var len = 0;
                while (len < minSize)
                {
                    var next = generator._rnd.Next(0, _words.Count);
                    var word = _words[next];

                    var toCopy = Math.Min(word.Length, minSize - len);

                    word[..toCopy].CopyTo(span[len..]);

                    len += toCopy;

                    if (len < minSize)
                        span[len++] = ' ';
                }
            });
        }
    }
}