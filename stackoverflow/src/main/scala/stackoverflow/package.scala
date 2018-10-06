package object stackoverflow {
  type Question = Posting
  type Answer = Posting
  type QID = Int
  type HighScore = Int
  type LangIndex = Int
  type MeanIndex = Int
  type Percent = Double
  type Language = String
  type Score = (LangIndex, HighScore)
}
